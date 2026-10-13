package com.mega.order.global.config;

/**
 * Saga 메시징에 쓰는 RabbitMQ 이름(exchange, 큐, routing key)을 한 곳에 모은다.
 * 큐 이름은 "받는 도메인.이벤트" 형식이다. 같은 이벤트라도 받는 도메인마다 큐를 따로 두어야
 * 각 도메인이 메시지를 하나씩 받는다 (한 큐를 같이 쓰면 둘 중 하나만 받는다).
 */
public final class SagaTopology {

	public static final String EXCHANGE = "mega.saga";

	public static final String ORDER_CREATED_ROUTING_KEY = "order.created";
	public static final String STOCK_RESERVED_ROUTING_KEY = "stock.reserved";
	public static final String PAYMENT_COMPLETED_ROUTING_KEY = "payment.completed";

	public static final String PRODUCT_ORDER_CREATED_QUEUE = "product.order-created";
	public static final String PAYMENT_STOCK_RESERVED_QUEUE = "payment.stock-reserved";
	public static final String ORDER_STOCK_RESERVED_QUEUE = "order.stock-reserved";
	public static final String ORDER_PAYMENT_COMPLETED_QUEUE = "order.payment-completed";

	private SagaTopology() {
	}

}
