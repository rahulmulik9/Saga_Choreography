package com.rahul.orderservice.event.producerEvent;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

import java.util.List;

@Getter
@Setter
@NoArgsConstructor
@AllArgsConstructor
public class ReleaseInventoryEvent {

    private Long orderId;

    private List<OrderItemPayload> items;
}