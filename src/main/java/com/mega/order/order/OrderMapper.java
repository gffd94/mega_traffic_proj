package com.mega.order.order;

import com.mega.order.order.dto.OrderCreateRequest;
import com.mega.order.order.dto.OrderItemRequest;
import com.mega.order.order.dto.OrderResponse;
import com.mega.order.order.dto.OrderStatusResponse;
import java.math.BigDecimal;
import org.springframework.stereotype.Component;

@Component
public class OrderMapper {

	public Order toEntity(String idempotencyKey, OrderCreateRequest request, BigDecimal totalPrice) {
		return Order.builder()
				.userId(request.userId())
				.idempotencyKey(idempotencyKey)
				.totalPrice(totalPrice)
				.build();
	}

	public OrderItem toItemEntity(Order order, OrderItemRequest request, BigDecimal unitPrice) {
		return OrderItem.builder()
				.order(order)
				.productId(request.productId())
				.quantity(request.quantity())
				.unitPrice(unitPrice)
				.build();
	}

	public OrderResponse toResponse(Order order) {
		return new OrderResponse(
				order.getId(),
				order.getStatus()
		);
	}

	public OrderStatusResponse toStatusResponse(Order order) {
		return new OrderStatusResponse(
				order.getId(),
				order.getStatus(),
				order.getCancelReason()
		);
	}

}
