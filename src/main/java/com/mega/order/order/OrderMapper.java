package com.mega.order.order;

import com.mega.order.order.dto.OrderCreateRequest;
import com.mega.order.order.dto.OrderItemRequest;
import com.mega.order.order.dto.OrderResponse;
import com.mega.order.order.dto.OrderStatusResponse;
import com.mega.order.order.event.OrderCreatedEvent;
import java.math.BigDecimal;
import java.util.List;
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

	public OrderCreatedEvent toCreatedEvent(Order order, List<OrderItem> orderItems) {
		List<OrderCreatedEvent.Item> items = orderItems.stream()
				.map(orderItem -> new OrderCreatedEvent.Item(orderItem.getProductId(), orderItem.getQuantity()))
				.toList();
		return new OrderCreatedEvent(
				order.getId(),
				order.getUserId(),
				order.getTotalPrice(),
				items
		);
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
