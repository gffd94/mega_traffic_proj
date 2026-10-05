package com.mega.order.user;

import com.mega.order.user.dto.UserCreateRequest;
import com.mega.order.user.dto.UserPointResponse;
import com.mega.order.user.dto.UserResponse;
import org.springframework.stereotype.Component;

@Component
public class UserMapper {

	public User toEntity(UserCreateRequest request) {
		return User.builder()
				.name(request.name())
				.build();
	}

	public UserResponse toResponse(User user) {
		return new UserResponse(
				user.getId(),
				user.getName(),
				user.getPointBalance()
		);
	}

	public UserPointResponse toPointResponse(User user) {
		return new UserPointResponse(
				user.getId(),
				user.getPointBalance()
		);
	}

}
