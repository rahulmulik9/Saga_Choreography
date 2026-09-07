package com.rahul.inventoryservice.listener;

import com.rahul.inventoryservice.entity.Product;
import com.rahul.inventoryservice.entity.ProcessedEvent;
import com.rahul.inventoryservice.event.KafkaTopicConfig;
import com.rahul.inventoryservice.event.consumerEvent.OrderItemPayload;
import com.rahul.inventoryservice.event.consumerEvent.OrderPendingEvent;
import com.rahul.inventoryservice.event.consumerEvent.ReleaseInventoryEvent;
import com.rahul.inventoryservice.event.producerEvent.InventoryRejectedEvent;
import com.rahul.inventoryservice.event.producerEvent.InventoryReservedEvent;
import com.rahul.inventoryservice.exception.InsufficientStockException;
import com.rahul.inventoryservice.repository.ProcessedEventRepository;
import com.rahul.inventoryservice.service.ProductService;
import jakarta.transaction.Transactional;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

import java.time.LocalDateTime;
import java.util.NoSuchElementException;

@Component
@RequiredArgsConstructor
@Slf4j
public class OrderEventListener {

    private final ProductService productService;
    private final KafkaTemplate<String, Object> kafkaTemplate;
    private final ProcessedEventRepository processedEventRepository;

    @KafkaListener(topics = KafkaTopicConfig.ORDER_PENDING, groupId = "inventory-service")
    @Transactional
    public void handleOrderPending(OrderPendingEvent event) {
        String key = event.getOrderId() + ":order.pending";
        if (processedEventRepository.existsByEventKey(key)) {
            log.info("Duplicate OrderPendingEvent for order {} — already processed, skipping", event.getOrderId());
            return;
        }

        try {
            for (OrderItemPayload line : event.getItems()) {
                Product product;
                try {
                    product = productService.getProductById(line.getProductId());
                } catch (NoSuchElementException ex) {
                    throw new InsufficientStockException(
                            "Product not found with id: " + line.getProductId());
                }
                if (product.getQuantity() < line.getQuantity()) {
                    throw new InsufficientStockException(
                            "Insufficient stock for product id " + line.getProductId()
                                    + ": requested " + line.getQuantity()
                                    + ", available " + product.getQuantity());
                }
            }

            for (OrderItemPayload line : event.getItems()) {
                productService.deductStock(line.getProductId(), line.getQuantity());
            }

            processedEventRepository.save(new ProcessedEvent(null, key, LocalDateTime.now()));

            kafkaTemplate.send(KafkaTopicConfig.INVENTORY_RESERVED,
                    new InventoryReservedEvent(event.getOrderId()));

        } catch (InsufficientStockException ex) {
            log.warn("Inventory rejected for order {}: {}", event.getOrderId(), ex.getMessage());
            processedEventRepository.save(new ProcessedEvent(null, key, LocalDateTime.now()));
            kafkaTemplate.send(KafkaTopicConfig.INVENTORY_REJECTED,
                    new InventoryRejectedEvent(event.getOrderId(), ex.getMessage()));
        }
    }

    @KafkaListener(topics = KafkaTopicConfig.RELEASE_INVENTORY, groupId = "inventory-service")
    @Transactional
    public void handleReleaseInventory(ReleaseInventoryEvent event) {
        String key = event.getOrderId() + ":inventory.release";
        if (processedEventRepository.existsByEventKey(key)) {
            log.info("Duplicate ReleaseInventoryEvent for order {} — already processed, skipping", event.getOrderId());
            return;
        }

        for (OrderItemPayload line : event.getItems()) {
            productService.restoreStock(line.getProductId(), line.getQuantity());
        }

        processedEventRepository.save(new ProcessedEvent(null, key, LocalDateTime.now()));
        log.info("Inventory released for order {}", event.getOrderId());
    }
}