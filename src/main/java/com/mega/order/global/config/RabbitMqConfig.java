package com.mega.order.global.config;

import static com.mega.order.global.config.SagaTopology.EXCHANGE;
import static com.mega.order.global.config.SagaTopology.ORDER_CREATED_ROUTING_KEY;
import static com.mega.order.global.config.SagaTopology.ORDER_PAYMENT_COMPLETED_QUEUE;
import static com.mega.order.global.config.SagaTopology.ORDER_STOCK_RESERVED_QUEUE;
import static com.mega.order.global.config.SagaTopology.PAYMENT_COMPLETED_ROUTING_KEY;
import static com.mega.order.global.config.SagaTopology.PAYMENT_STOCK_RESERVED_QUEUE;
import static com.mega.order.global.config.SagaTopology.PRODUCT_ORDER_CREATED_QUEUE;
import static com.mega.order.global.config.SagaTopology.STOCK_RESERVED_ROUTING_KEY;

import lombok.extern.slf4j.Slf4j;
import org.springframework.amqp.AmqpException;
import org.springframework.amqp.core.AmqpAdmin;
import org.springframework.amqp.core.Binding;
import org.springframework.amqp.core.BindingBuilder;
import org.springframework.amqp.core.DirectExchange;
import org.springframework.amqp.core.ExchangeBuilder;
import org.springframework.amqp.core.Queue;
import org.springframework.amqp.core.QueueBuilder;
import org.springframework.boot.ApplicationRunner;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * 여기 선언한 Exchange/Queue/Binding 빈은 RabbitAdmin(Spring Boot 자동 구성)이
 * RabbitMQ 연결이 맺어질 때마다 브로커에 선언한다. 같은 설정으로 이미 있으면 그대로 둔다.
 */
@Slf4j
@Configuration
public class RabbitMqConfig {

	/**
	 * 아직 컨슈머가 없어서, 그냥 두면 첫 메시지를 보낼 때까지 연결이 맺어지지 않아 큐가 생기지 않는다.
	 * 메시지가 오기 전에도 큐가 있어야 다른 도메인이 보낸 메시지가 버려지지 않으므로 기동 시점에 선언한다.
	 * RabbitMQ가 내려가 있어도 앱 기동은 막지 않는다. 나중에 연결이 맺어질 때 RabbitAdmin이 다시 선언한다.
	 */
	@Bean
	public ApplicationRunner sagaTopologyDeclarer(AmqpAdmin amqpAdmin) {
		return args -> {
			try {
				amqpAdmin.initialize();
			} catch (AmqpException e) {
				log.warn("기동 시 RabbitMQ 토폴로지 선언 실패. 연결이 맺어질 때 다시 선언된다.", e);
			}
		};
	}

	@Bean
	public DirectExchange sagaExchange() {
		return ExchangeBuilder.directExchange(EXCHANGE).durable(true).build();
	}

	@Bean
	public Queue productOrderCreatedQueue() {
		return QueueBuilder.durable(PRODUCT_ORDER_CREATED_QUEUE).build();
	}

	@Bean
	public Queue paymentStockReservedQueue() {
		return QueueBuilder.durable(PAYMENT_STOCK_RESERVED_QUEUE).build();
	}

	@Bean
	public Queue orderStockReservedQueue() {
		return QueueBuilder.durable(ORDER_STOCK_RESERVED_QUEUE).build();
	}

	@Bean
	public Queue orderPaymentCompletedQueue() {
		return QueueBuilder.durable(ORDER_PAYMENT_COMPLETED_QUEUE).build();
	}

	@Bean
	public Binding productOrderCreatedBinding() {
		return BindingBuilder.bind(productOrderCreatedQueue()).to(sagaExchange()).with(ORDER_CREATED_ROUTING_KEY);
	}

	@Bean
	public Binding paymentStockReservedBinding() {
		return BindingBuilder.bind(paymentStockReservedQueue()).to(sagaExchange()).with(STOCK_RESERVED_ROUTING_KEY);
	}

	@Bean
	public Binding orderStockReservedBinding() {
		return BindingBuilder.bind(orderStockReservedQueue()).to(sagaExchange()).with(STOCK_RESERVED_ROUTING_KEY);
	}

	@Bean
	public Binding orderPaymentCompletedBinding() {
		return BindingBuilder.bind(orderPaymentCompletedQueue()).to(sagaExchange()).with(PAYMENT_COMPLETED_ROUTING_KEY);
	}

}
