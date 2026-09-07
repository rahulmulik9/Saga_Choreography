package com.rahul.inventoryservice.config;

public final class KafkaTopicConfig {

    private KafkaTopicConfig() {
    }

    public static final String ORDER_PENDING = "order.pending";
    public static final String INVENTORY_RESERVED = "inventory.reserved";
    public static final String INVENTORY_REJECTED = "inventory.rejected";
}