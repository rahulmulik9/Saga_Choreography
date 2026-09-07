package com.rahul.paymentservice.event;

public final class KafkaTopicConfig {

    private KafkaTopicConfig() {
    }

    public static final String PAYMENT_REQUESTED = "payment.requested";
    public static final String PAYMENT_SUCCESSFUL = "payment.successful";
    public static final String PAYMENT_FAILED = "payment.failed";
}