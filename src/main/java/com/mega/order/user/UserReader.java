package com.mega.order.user;

import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Component;

@Component
@RequiredArgsConstructor
public class UserReader {

	private final UserRepository userRepository;

	public boolean existsById(Long userId) {
		return userRepository.existsById(userId);
	}

}
