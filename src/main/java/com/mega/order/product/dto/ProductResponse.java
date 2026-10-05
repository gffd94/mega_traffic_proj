package com.mega.order.product.dto;

import java.math.BigDecimal;

public record ProductResponse(
		Long id,
		String name,
		BigDecimal price,
		int stockQuantity
) {
}
