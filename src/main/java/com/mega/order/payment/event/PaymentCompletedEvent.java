package com.mega.order.payment.event;

public record PaymentCompletedEvent(
		Long orderId
) {
}
