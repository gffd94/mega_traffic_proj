package com.mega.order.order.dto;

import com.mega.order.order.OrderStatus;

public record OrderResponse(
		Long orderId,
		OrderStatus status
) {
}
