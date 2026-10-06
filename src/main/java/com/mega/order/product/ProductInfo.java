package com.mega.order.product;

import java.math.BigDecimal;

public record ProductInfo(
		Long id,
		BigDecimal price
) {
}
