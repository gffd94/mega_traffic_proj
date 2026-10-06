package com.mega.order.order.dto;

import com.mega.order.order.CancelReason;
import com.mega.order.order.OrderStatus;

public record OrderStatusResponse(
		Long orderId,
		OrderStatus status,
		CancelReason cancelReason
) {
}
