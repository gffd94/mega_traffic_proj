package com.mega.order.global.config;

import static com.mega.order.global.config.SagaTopology.DEAD_LETTER_EXCHANGE;
import static com.mega.order.global.config.SagaTopology.DEAD_LETTER_QUEUE;
import static com.mega.order.global.config.SagaTopology.EXCHANGE;
import static com.mega.order.global.config.SagaTopology.ORDER_CREATED_ROUTING_KEY;
import static com.mega.order.global.config.SagaTopology.ORDER_PAYMENT_COMPLETED_QUEUE;
import static com.mega.order.global.config.SagaTopology.ORDER_PAYMENT_FAILED_QUEUE;
import static com.mega.order.global.config.SagaTopology.ORDER_STOCK_RESERVED_QUEUE;
import static com.mega.order.global.config.SagaTopology.ORDER_STOCK_RESERVE_FAILED_QUEUE;
import static com.mega.order.global.config.SagaTopology.PAYMENT_COMPLETED_ROUTING_KEY;
import static com.mega.order.global.config.SagaTopology.PAYMENT_FAILED_ROUTING_KEY;
import static com.mega.order.global.config.SagaTopology.PAYMENT_STOCK_RESERVED_QUEUE;
import static com.mega.order.global.config.SagaTopology.PRODUCT_ORDER_CREATED_QUEUE;
import static com.mega.order.global.config.SagaTopology.PRODUCT_PAYMENT_FAILED_QUEUE;
import static com.mega.order.global.config.SagaTopology.STOCK_RESERVED_ROUTING_KEY;
import static com.mega.order.global.config.SagaTopology.STOCK_RESERVE_FAILED_ROUTING_KEY;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.Map;
import org.springframework.amqp.core.Binding;
import org.springframework.amqp.core.BindingBuilder;
import org.springframework.amqp.core.DirectExchange;
import org.springframework.amqp.core.ExchangeBuilder;
import org.springframework.amqp.core.FanoutExchange;
import org.springframework.amqp.core.Queue;
import org.springframework.amqp.core.QueueBuilder;
import org.springframework.amqp.support.converter.Jackson2JsonMessageConverter;
import org.springframework.amqp.support.converter.MessageConverter;
import org.springframework.boot.autoconfigure.amqp.RabbitProperties;
import org.springframework.boot.autoconfigure.amqp.RabbitRetryTemplateCustomizer;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.messaging.MessageHandlingException;
import org.springframework.messaging.handler.invocation.MethodArgumentResolutionException;
import org.springframework.retry.policy.SimpleRetryPolicy;

/**
 * 여기 선언한 Exchange/Queue/Binding 빈은 RabbitAdmin(Spring Boot 자동 구성)이
 * RabbitMQ 연결이 맺어질 때마다 브로커에 선언한다. 같은 설정으로 이미 있으면 그대로 둔다.
 * 컨슈머(@RabbitListener)가 기동 시점에 연결을 맺으므로 앱이 뜨면 바로 선언된다.
 * 이미 있는 큐와 인자(x-dead-letter-exchange 등)가 다르면 선언이 PRECONDITION_FAILED로 실패하므로,
 * 큐 인자를 바꿀 때는 브로커의 기존 큐를 지우고 다시 만들어야 한다 (README 10장 참고).
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

	/**
	 * 메시지 자체가 잘못된 경우(본문을 이벤트로 변환할 수 없음, 필수 헤더 eventId 없음)는 다시 시도해도 결과가 같다.
	 * 이런 예외는 재시도하지 않고 바로 SagaMessageRecoverer로 넘겨 DLQ로 보낸다. 그 밖의 예외는 설정대로 재시도한다.
	 * 리스너 예외는 ListenerExecutionFailedException으로 감싸져 오므로 원인(cause)까지 따라가며 판별한다.
	 */
	@Bean
	public RabbitRetryTemplateCustomizer nonRetryableMessageErrors(RabbitProperties rabbitProperties) {
		int maxAttempts = rabbitProperties.getListener().getSimple().getRetry().getMaxAttempts();
		return (target, retryTemplate) -> {
			if (target != RabbitRetryTemplateCustomizer.Target.LISTENER) {
				return;
			}
			retryTemplate.setRetryPolicy(new SimpleRetryPolicy(maxAttempts, Map.of(
					// 본문 역직렬화 실패
					org.springframework.amqp.support.converter.MessageConversionException.class, false,
					org.springframework.messaging.converter.MessageConversionException.class, false,
					// 필수 헤더 없음(HeaderMethodArgumentResolver가 던짐), 파라미터 해석 실패
					MessageHandlingException.class, false,
					MethodArgumentResolutionException.class, false
			), true, true));
		};
	}

	@Bean
	public DirectExchange sagaExchange() {
		return ExchangeBuilder.directExchange(EXCHANGE).durable(true).build();
	}

	// DLQ가 하나뿐이라 routing key와 관계없이 모두 보내도록 fanout으로 둔다
	@Bean
	public FanoutExchange deadLetterExchange() {
		return ExchangeBuilder.fanoutExchange(DEAD_LETTER_EXCHANGE).durable(true).build();
	}

	@Bean
	public Queue deadLetterQueue() {
		return QueueBuilder.durable(DEAD_LETTER_QUEUE).build();
	}

	@Bean
	public Binding deadLetterBinding() {
		return BindingBuilder.bind(deadLetterQueue()).to(deadLetterExchange());
	}

	@Bean
	public Queue productOrderCreatedQueue() {
		return sagaQueue(PRODUCT_ORDER_CREATED_QUEUE);
	}

	@Bean
	public Queue paymentStockReservedQueue() {
		return sagaQueue(PAYMENT_STOCK_RESERVED_QUEUE);
	}

	@Bean
	public Queue orderStockReservedQueue() {
		return sagaQueue(ORDER_STOCK_RESERVED_QUEUE);
	}

	@Bean
	public Queue orderPaymentCompletedQueue() {
		return sagaQueue(ORDER_PAYMENT_COMPLETED_QUEUE);
	}

	@Bean
	public Queue orderStockReserveFailedQueue() {
		return sagaQueue(ORDER_STOCK_RESERVE_FAILED_QUEUE);
	}

	@Bean
	public Queue productPaymentFailedQueue() {
		return sagaQueue(PRODUCT_PAYMENT_FAILED_QUEUE);
	}

	@Bean
	public Queue orderPaymentFailedQueue() {
		return sagaQueue(ORDER_PAYMENT_FAILED_QUEUE);
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

	@Bean
	public Binding orderStockReserveFailedBinding() {
		return BindingBuilder.bind(orderStockReserveFailedQueue()).to(sagaExchange()).with(STOCK_RESERVE_FAILED_ROUTING_KEY);
	}

	@Bean
	public Binding productPaymentFailedBinding() {
		return BindingBuilder.bind(productPaymentFailedQueue()).to(sagaExchange()).with(PAYMENT_FAILED_ROUTING_KEY);
	}

	@Bean
	public Binding orderPaymentFailedBinding() {
		return BindingBuilder.bind(orderPaymentFailedQueue()).to(sagaExchange()).with(PAYMENT_FAILED_ROUTING_KEY);
	}

	// reject(requeue=false)된 메시지는 x-dead-letter-exchange로 옮겨진다
	private Queue sagaQueue(String name) {
		return QueueBuilder.durable(name).deadLetterExchange(DEAD_LETTER_EXCHANGE).build();
	}

}
