package com.mega.order.global.exception;

public class OrderNotFoundException extends RuntimeException {

	public OrderNotFoundException(Long orderId) {
		super("주문을 찾을 수 없습니다. orderId: " + orderId);
	}

}
