package com.mega.order.user;

import com.mega.order.global.exception.UserNotFoundException;
import com.mega.order.user.dto.PointChargeRequest;
import com.mega.order.user.dto.UserCreateRequest;
import com.mega.order.user.dto.UserPointResponse;
import com.mega.order.user.dto.UserResponse;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

@Service
@RequiredArgsConstructor
public class UserService {

	private final UserRepository userRepository;
	private final UserMapper userMapper;

	@Transactional
	public UserResponse create(UserCreateRequest request) {
		User user = userMapper.toEntity(request);
		User savedUser = userRepository.save(user);
		return userMapper.toResponse(savedUser);
	}

	@Transactional
	public UserPointResponse chargePoint(Long userId, PointChargeRequest request) {
		User user = findUser(userId);
		user.chargePoint(request.amount());
		return userMapper.toPointResponse(user);
	}

	@Transactional(readOnly = true)
	public UserPointResponse getPoint(Long userId) {
		User user = findUser(userId);
		return userMapper.toPointResponse(user);
	}

	private User findUser(Long userId) {
		return userRepository.findById(userId)
				.orElseThrow(() -> new UserNotFoundException(userId));
	}

}
