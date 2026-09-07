package com.rahul.paymentservice.listener;

import com.rahul.paymentservice.entity.Payment;
import com.rahul.paymentservice.entity.PaymentStatus;
import com.rahul.paymentservice.event.KafkaTopicConfig;
import com.rahul.paymentservice.event.consumerEvent.PaymentRequestedEvent;
import com.rahul.paymentservice.event.producerEvent.PaymentFailedEvent;
import com.rahul.paymentservice.event.producerEvent.PaymentSuccessfulEvent;
import com.rahul.paymentservice.service.PaymentService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

@Component
@RequiredArgsConstructor
@Slf4j
public class PaymentEventListener {

    private final PaymentService paymentService;
    private final KafkaTemplate<String, Object> kafkaTemplate;

    @KafkaListener(topics = KafkaTopicConfig.PAYMENT_REQUESTED, groupId = "payment-service")
    public void handlePaymentRequested(PaymentRequestedEvent event) {
        Payment payment = paymentService.makePayment(event.getOrderId(), event.getAmount());

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