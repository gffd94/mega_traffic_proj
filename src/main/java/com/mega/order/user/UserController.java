package com.mega.order.user;

import com.mega.order.user.dto.PointChargeRequest;
import com.mega.order.user.dto.UserCreateRequest;
import com.mega.order.user.dto.UserPointResponse;
import com.mega.order.user.dto.UserResponse;
import jakarta.validation.Valid;
import lombok.RequiredArgsConstructor;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/api/v1/users")
@RequiredArgsConstructor
public class UserController {

	private final UserService userService;

	@PostMapping
	public ResponseEntity<UserResponse> create(@Valid @RequestBody UserCreateRequest request) {
		UserResponse response = userService.create(request);
		return ResponseEntity.status(HttpStatus.CREATED).body(response);
	}

	@PostMapping("/{userId}/points")
	public ResponseEntity<UserPointResponse> chargePoint(@PathVariable Long userId,
			@Valid @RequestBody PointChargeRequest request) {
		UserPointResponse response = userService.chargePoint(userId, request);
		return ResponseEntity.ok(response);
	}

	@GetMapping("/{userId}/points")
	public ResponseEntity<UserPointResponse> getPoint(@PathVariable Long userId) {
		UserPointResponse response = userService.getPoint(userId);
		return ResponseEntity.ok(response);
	}

}
