package com.rahul.orderservice.event.consumerEvent;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

@Getter
@Setter
@NoArgsConstructor
@AllArgsConstructor
public class InventoryRejectedEvent {

    private Long orderId;

    private String reason;
}