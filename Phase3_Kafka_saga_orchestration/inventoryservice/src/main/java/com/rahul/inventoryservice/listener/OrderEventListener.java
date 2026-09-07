package com.rahul.inventoryservice.listener;

import com.rahul.inventoryservice.config.KafkaTopicConfig;
import com.rahul.inventoryservice.entity.Product;
import com.rahul.inventoryservice.event.consumerEvent.OrderItemPayload;
import com.rahul.inventoryservice.event.consumerEvent.OrderPendingEvent;
import com.rahul.inventoryservice.event.producerEvent.InventoryRejectedEvent;
import com.rahul.inventoryservice.event.producerEvent.InventoryReservedEvent;
import com.rahul.inventoryservice.exception.InsufficientStockException;
import com.rahul.inventoryservice.service.ProductService;
import jakarta.transaction.Transactional;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

import java.util.NoSuchElementException;

@Component
@RequiredArgsConstructor
@Slf4j
public class OrderEventListener {

    private final ProductService productService;
    private final KafkaTemplate<String, Object> kafkaTemplate;

    @KafkaListener(topics = KafkaTopicConfig.ORDER_PENDING, groupId = "inventory-service")
    @Transactional
    public void handleOrderPending(OrderPendingEvent event) {
        try {
            // Validate all items have sufficient stock BEFORE deducting any,
            // same reasoning as Phase 1: avoid partial deduction on a mid-loop failure.
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

            kafkaTemplate.send(KafkaTopicConfig.INVENTORY_RESERVED,
                    new InventoryReservedEvent(event.getOrderId()));

        } catch (InsufficientStockException ex) {
            log.warn("Inventory rejected for order {}: {}", event.getOrderId(), ex.getMessage());
            kafkaTemplate.send(KafkaTopicConfig.INVENTORY_REJECTED,
                    new InventoryRejectedEvent(event.getOrderId(), ex.getMessage()));
        }
    }
}