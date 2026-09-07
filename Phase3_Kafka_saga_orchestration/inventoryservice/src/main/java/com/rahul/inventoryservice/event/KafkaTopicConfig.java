package com.rahul.inventoryservice.event;

public final class KafkaTopicConfig {

    private KafkaTopicConfig() {
    }

    public static final String ORDER_PENDING = "order.pending";
    public static final String INVENTORY_RESERVED = "inventory.reserved";
    public static final String INVENTORY_REJECTED = "inventory.rejected";
    public static final String RELEASE_INVENTORY = "inventory.release";
}