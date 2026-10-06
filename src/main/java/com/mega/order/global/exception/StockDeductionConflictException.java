package com.mega.order.global.exception;

public class StockDeductionConflictException extends RuntimeException {

	public StockDeductionConflictException(Long productId, int maxRetries, Throwable cause) {
		super("재고 차감 충돌로 재시도 " + maxRetries + "회를 모두 실패했습니다. productId: " + productId, cause);
	}

}
