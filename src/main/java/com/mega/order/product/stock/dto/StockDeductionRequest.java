package com.mega.order.product.stock.dto;

import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Positive;

public record StockDeductionRequest(
		@NotNull Long productId,
		@NotNull @Positive Integer quantity
) {
}
