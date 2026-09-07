package com.rahul.orderservice.listener;

import com.rahul.orderservice.entity.Order;
import com.rahul.orderservice.entity.OrderStatus;
import com.rahul.orderservice.entity.ProcessedEvent;
import com.rahul.orderservice.event.KafkaTopicConfig;
import com.rahul.orderservice.event.consumerEvent.PaymentFailedEvent;
import com.rahul.orderservice.event.consumerEvent.PaymentSuccessfulEvent;
import com.rahul.orderservice.event.producerEvent.OrderItemPayload;
import com.rahul.orderservice.event.producerEvent.ReleaseInventoryEvent;
import com.rahul.orderservice.repository.OrderRepository;
import com.rahul.orderservice.repository.ProcessedEventRepository;
import jakarta.transaction.Transactional;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

import java.time.LocalDateTime;
import java.util.List;
import java.util.NoSuchElementException;

@Component
@RequiredArgsConstructor
@Slf4j
public class PaymentEventListener {

    private final OrderRepository orderRepository;
    private final KafkaTemplate<String, Object> kafkaTemplate;
    private final ProcessedEventRepository processedEventRepository;

    @KafkaListener(topics = KafkaTopicConfig.PAYMENT_SUCCESSFUL, groupId = "order-service")
    @Transactional
    public void handlePaymentSuccessful(PaymentSuccessfulEvent event) {
        String key = event.getOrderId() + ":payment.successful";
        if (processedEventRepository.existsByEventKey(key)) {
            log.info("Duplicate PaymentSuccessfulEvent for order {} — skipping", event.getOrderId());
            return;
        }

        Order order = orderRepository.findById(event.getOrderId())
                .orElseThrow(() -> new NoSuchElementException(
                        "Order not found with id: " + event.getOrderId()));

        order.setStatus(OrderStatus.COMPLETED);
        orderRepository.save(order);

        processedEventRepository.save(new ProcessedEvent(null, key, LocalDateTime.now()));
    }

    @KafkaListener(topics = KafkaTopicConfig.PAYMENT_FAILED, groupId = "order-service")
    @Transactional
    public void handlePaymentFailed(PaymentFailedEvent event) {
        String key = event.getOrderId() + ":payment.failed";
        if (processedEventRepository.existsByEventKey(key)) {
            log.info("Duplicate PaymentFailedEvent for order {} — skipping", event.getOrderId());
            return;
        }

        Order order = orderRepository.findById(event.getOrderId())
                .orElseThrow(() -> new NoSuchElementException(
                        "Order not found with id: " + event.getOrderId()));

        order.setStatus(OrderStatus.FAILED);
        orderRepository.save(order);

        List<OrderItemPayload> items = order.getItems().stream()
                .map(item -> new OrderItemPayload(item.getProductId(), item.getQuantity()))
                .toList();

        processedEventRepository.save(new ProcessedEvent(null, key, LocalDateTime.now()));

        kafkaTemplate.send(KafkaTopicConfig.RELEASE_INVENTORY,
                new ReleaseInventoryEvent(order.getId(), items));

        log.warn("Order {} marked FAILED, releasing inventory: {}", event.getOrderId(), event.getReason());
    }
}