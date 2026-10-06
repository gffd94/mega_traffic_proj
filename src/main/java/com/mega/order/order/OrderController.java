package com.mega.order.order;

import com.mega.order.order.dto.OrderCreateRequest;
import com.mega.order.order.dto.OrderResponse;
import com.mega.order.order.dto.OrderStatusResponse;
import jakarta.validation.Valid;
import lombok.RequiredArgsConstructor;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestHeader;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/api/v1/orders")
@RequiredArgsConstructor
public class OrderController {

	private static final int IDEMPOTENCY_KEY_MAX_LENGTH = 64;

	private final OrderFacade orderFacade;
	private final OrderService orderService;

	@PostMapping
	public ResponseEntity<OrderResponse> create(@RequestHeader("Idempotency-Key") String idempotencyKey,
			@Valid @RequestBody OrderCreateRequest request) {
		validateIdempotencyKey(idempotencyKey);
		OrderResponse response = orderFacade.create(idempotencyKey, request);
		return ResponseEntity.status(HttpStatus.ACCEPTED).body(response);
	}

	@GetMapping("/{orderId}/status")
	public ResponseEntity<OrderStatusResponse> getStatus(@PathVariable Long orderId) {
		OrderStatusResponse response = orderService.getStatus(orderId);
		return ResponseEntity.ok(response);
	}

	private void validateIdempotencyKey(String idempotencyKey) {
		if (idempotencyKey.isBlank() || idempotencyKey.length() > IDEMPOTENCY_KEY_MAX_LENGTH) {
			throw new IllegalArgumentException(
					"Idempotency-Key는 공백일 수 없고 " + IDEMPOTENCY_KEY_MAX_LENGTH + "자 이하여야 합니다.");
		}
	}

}
