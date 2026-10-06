package com.mega.order.global.exception;

import java.util.List;

public class ProductNotFoundException extends RuntimeException {

	public ProductNotFoundException(List<Long> productIds) {
		super("상품을 찾을 수 없습니다. productIds: " + productIds);
	}

}
