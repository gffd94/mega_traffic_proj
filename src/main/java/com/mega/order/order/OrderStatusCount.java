package com.mega.order.order;

public record OrderStatusCount(
		OrderStatus status,
		Long count
) {
}
