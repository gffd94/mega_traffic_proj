package com.mega.order.order.event;

import java.math.BigDecimal;
import java.util.List;

public record OrderCreatedEvent(
		Long orderId,
		Long userId,
		BigDecimal totalPrice,
		List<Item> items
) {

	public record Item(
			Long productId,
			int quantity
	) {
	}

}
