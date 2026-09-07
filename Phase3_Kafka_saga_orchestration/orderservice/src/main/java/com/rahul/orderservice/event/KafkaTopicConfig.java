package com.rahul.orderservice.event;

public final class KafkaTopicConfig {

    private KafkaTopicConfig() {
    }

    public static final String ORDER_PENDING = "order.pending";
    public static final String PAYMENT_REQUESTED = "payment.requested";
    public static final String RELEASE_INVENTORY = "inventory.release";

    public static final String INVENTORY_RESERVED = "inventory.reserved";
    public static final String INVENTORY_REJECTED = "inventory.rejected";
    public static final String PAYMENT_SUCCESSFUL = "payment.successful";
    public static final String PAYMENT_FAILED = "payment.failed";
}