package com.mega.order.global.exception;

public class InsufficientStockException extends RuntimeException {

	public InsufficientStockException(int requestedQuantity, int currentStock) {
		super("재고가 부족합니다. 요청 수량: " + requestedQuantity + ", 현재 재고: " + currentStock);
	}

}
