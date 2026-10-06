package com.mega.order.product.stock.dto;

public record StockDeductionResponse(
		Long productId,
		String strategy
) {
}
