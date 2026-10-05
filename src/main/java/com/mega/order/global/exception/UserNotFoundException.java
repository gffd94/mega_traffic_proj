package com.mega.order.global.exception;

public class UserNotFoundException extends RuntimeException {

	public UserNotFoundException(Long userId) {
		super("회원을 찾을 수 없습니다. userId: " + userId);
	}

}
