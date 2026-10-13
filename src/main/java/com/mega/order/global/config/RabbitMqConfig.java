package com.mega.order.global.config;

import static com.mega.order.global.config.SagaTopology.EXCHANGE;
import static com.mega.order.global.config.SagaTopology.ORDER_CREATED_ROUTING_KEY;
import static com.mega.order.global.config.SagaTopology.ORDER_PAYMENT_COMPLETED_QUEUE;
import static com.mega.order.global.config.SagaTopology.ORDER_STOCK_RESERVED_QUEUE;
import static com.mega.order.global.config.SagaTopology.PAYMENT_COMPLETED_ROUTING_KEY;
import static com.mega.order.global.config.SagaTopology.PAYMENT_STOCK_RESERVED_QUEUE;
import static com.mega.order.global.config.SagaTopology.PRODUCT_ORDER_CREATED_QUEUE;
import static com.mega.order.global.config.SagaTopology.STOCK_RESERVED_ROUTING_KEY;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.amqp.core.Binding;
import org.springframework.amqp.core.BindingBuilder;
import org.springframework.amqp.core.DirectExchange;
import org.springframework.amqp.core.ExchangeBuilder;
import org.springframework.amqp.core.Queue;
import org.springframework.amqp.core.QueueBuilder;
import org.springframework.amqp.support.converter.Jackson2JsonMessageConverter;
import org.springframework.amqp.support.converter.MessageConverter;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * 여기 선언한 Exchange/Queue/Binding 빈은 RabbitAdmin(Spring Boot 자동 구성)이
 * RabbitMQ 연결이 맺어질 때마다 브로커에 선언한다. 같은 설정으로 이미 있으면 그대로 둔다.
 * 컨슈머(@RabbitListener)가 기동 시점에 연결을 맺으므로 앱이 뜨면 바로 선언된다.
 */
@Configuration
public class RabbitMqConfig {

	/**
	 * @RabbitListener 메서드의 파라미터 타입(이벤트 record)으로 JSON body를 역직렬화한다.
	 * Spring이 관리하는 ObjectMapper를 써서 HTTP 쪽과 같은 Jackson 설정을 따른다.
	 * 금액(BigDecimal)은 JSON의 숫자 텍스트에서 바로 만들어지므로 double을 거치지 않는다.
	 * Relay는 이미 JSON 문자열인 payload를 Message로 직접 보내므로 이 변환기를 거치지 않는다.
	 */
	@Bean
	public MessageConverter jsonMessageConverter(ObjectMapper objectMapper) {
		return new Jackson2JsonMessageConverter(objectMapper);
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
