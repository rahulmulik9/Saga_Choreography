package com.rahul.paymentservice.listener;

import com.rahul.paymentservice.entity.Payment;
import com.rahul.paymentservice.entity.PaymentStatus;
import com.rahul.paymentservice.event.KafkaTopicConfig;
import com.rahul.paymentservice.event.ProcessedEvent;
import com.rahul.paymentservice.event.consumerEvent.PaymentRequestedEvent;
import com.rahul.paymentservice.event.producerEvent.PaymentFailedEvent;
import com.rahul.paymentservice.event.producerEvent.PaymentSuccessfulEvent;
import com.rahul.paymentservice.repository.ProcessedEventRepository;
import com.rahul.paymentservice.service.PaymentService;
import jakarta.transaction.Transactional;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

import java.time.LocalDateTime;

@Component
@RequiredArgsConstructor
@Slf4j
public class PaymentEventListener {

    private final PaymentService paymentService;
    private final KafkaTemplate<String, Object> kafkaTemplate;
    private final ProcessedEventRepository processedEventRepository;

    @KafkaListener(topics = KafkaTopicConfig.PAYMENT_REQUESTED, groupId = "payment-service")
    @Transactional
    public void handlePaymentRequested(PaymentRequestedEvent event) {
        String key = event.getOrderId() + ":payment.requested";
        if (processedEventRepository.existsByEventKey(key)) {
            log.info("Duplicate PaymentRequestedEvent for order {} — skipping", event.getOrderId());
            return;
        }

        Payment payment = paymentService.makePayment(event.getOrderId(), event.getAmount());
        processedEventRepository.save(new ProcessedEvent(null, key, LocalDateTime.now()));

        if (payment.getStatus() == PaymentStatus.SUCCESS) {
            kafkaTemplate.send(KafkaTopicConfig.PAYMENT_SUCCESSFUL,
                    new PaymentSuccessfulEvent(event.getOrderId()));
        } else {
            log.warn("Payment failed for order {}: amount {}", event.getOrderId(), event.getAmount());
            kafkaTemplate.send(KafkaTopicConfig.PAYMENT_FAILED,
                    new PaymentFailedEvent(event.getOrderId(), "Payment declined for amount: " + event.getAmount()));
        }
    }
}