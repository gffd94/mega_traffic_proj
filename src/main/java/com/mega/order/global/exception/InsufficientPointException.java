package com.mega.order.global.exception;

import java.math.BigDecimal;

public class InsufficientPointException extends RuntimeException {

	public InsufficientPointException(BigDecimal requestedAmount, BigDecimal currentBalance) {
		super("포인트가 부족합니다. 요청 금액: " + requestedAmount + ", 현재 잔액: " + currentBalance);
	}

}
