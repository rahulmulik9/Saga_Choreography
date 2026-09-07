package com.rahul.orderservice.listener;

import com.rahul.orderservice.entity.Order;
import com.rahul.orderservice.entity.OrderStatus;
import com.rahul.orderservice.event.KafkaTopicConfig;
import com.rahul.orderservice.event.consumerEvent.InventoryRejectedEvent;
import com.rahul.orderservice.event.consumerEvent.InventoryReservedEvent;
import com.rahul.orderservice.event.producerEvent.PaymentRequestedEvent;
import com.rahul.orderservice.repository.OrderRepository;
import jakarta.transaction.Transactional;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

import java.math.BigDecimal;
import java.util.NoSuchElementException;

@Component
@RequiredArgsConstructor
@Slf4j
public class InventoryEventListener {

    private final OrderRepository orderRepository;
    private final KafkaTemplate<String, Object> kafkaTemplate;

    @KafkaListener(topics = KafkaTopicConfig.INVENTORY_RESERVED, groupId = "order-service")
    @Transactional
    public void handleInventoryReserved(InventoryReservedEvent event) {
        Order order = orderRepository.findById(event.getOrderId())
                .orElseThrow(() -> new NoSuchElementException(
                        "Order not found with id: " + event.getOrderId()));

        BigDecimal total = order.getItems().stream()
                .map(item -> item.getPrice().multiply(BigDecimal.valueOf(item.getQuantity())))
                .reduce(BigDecimal.ZERO, BigDecimal::add);

        kafkaTemplate.send(KafkaTopicConfig.PAYMENT_REQUESTED,
                new PaymentRequestedEvent(order.getId(), total));
    }

    @KafkaListener(topics = KafkaTopicConfig.INVENTORY_REJECTED, groupId = "order-service")
    @Transactional
    public void handleInventoryRejected(InventoryRejectedEvent event) {
        Order order = orderRepository.findById(event.getOrderId())
                .orElseThrow(() -> new NoSuchElementException(
                        "Order not found with id: " + event.getOrderId()));

        order.setStatus(OrderStatus.FAILED);
        orderRepository.save(order);

        log.warn("Order {} marked FAILED: {}", event.getOrderId(), event.getReason());
    }
}