package com.mega.order.global.outbox;

import com.mega.order.global.config.SagaTopology;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.concurrent.TimeUnit;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.amqp.core.Message;
import org.springframework.amqp.core.MessageProperties;
import org.springframework.amqp.rabbit.connection.CorrelationData;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

/**
 * READY 상태의 Outbox 이벤트를 주기적으로 RabbitMQ에 전송하고 PUBLISHED로 바꾼다.
 *
 * 전송과 PUBLISHED 표시는 하나의 트랜잭션으로 묶을 수 없다 (RabbitMQ와 DB는 별개 시스템).
 * 그래서 전송에 성공한 뒤 PUBLISHED 표시가 커밋되기 전에 서버가 죽으면, 이벤트는 READY로 남아
 * 재시작 후 같은 이벤트가 한 번 더 전송된다 (at-least-once).
 * 이 중복은 나중에 컨슈머가 eventId 헤더로 processed_events를 확인하는 멱등성 처리로 걸러낸다.
 *
 * 반대로 PUBLISHED로 먼저 바꾸고 전송하면, 그 사이에 죽었을 때 이벤트가 유실된다.
 * 유실은 되돌릴 방법이 없고 중복은 컨슈머에서 걸러낼 수 있으므로 "전송 후 표시" 순서를 택한다.
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class OutboxRelay {

	private static final int CONFIRM_TIMEOUT_SECONDS = 5;

	private final OutboxEventRepository outboxEventRepository;
	private final RabbitTemplate rabbitTemplate;

	// fixedDelay: 이전 실행이 끝나고 1초 뒤에 다음 실행. 전송이 느려져도 실행이 겹치지 않는다
	@Scheduled(fixedDelay = 1000)
	public void relay() {
		List<OutboxEvent> events = outboxEventRepository.findTop100ByStatusOrderByCreatedAtAscIdAsc(OutboxEventStatus.READY);
		for (OutboxEvent event : events) {
			try {
				send(event);
			} catch (Exception e) {
				// 이 이벤트는 READY로 남아 다음 주기에 다시 시도된다. 나머지 이벤트는 계속 처리한다
				log.warn("Outbox 이벤트 전송 실패. eventId={}, eventType={}", event.getEventId(), event.getEventType(), e);
				continue;
			}
			// 이 시점에 서버가 죽으면 전송은 됐지만 READY로 남아 재시작 후 중복 전송된다 (클래스 주석 참고)
			event.markPublished();
			outboxEventRepository.save(event);
		}
	}

	/**
	 * publisher confirm으로 브로커가 메시지를 받았는지, mandatory return으로 큐까지 라우팅됐는지 확인한다.
	 * 둘 중 하나라도 실패하면 예외를 던진다.
	 * (confirm 없이 send만 하면 브로커가 메시지를 받지 못했거나 버려도 성공처럼 보인다)
	 */
	private void send(OutboxEvent event) throws Exception {
		String routingKey = OutboxEventTypes.routingKeyOf(event.getEventType());
		CorrelationData correlationData = new CorrelationData(event.getEventId());

		rabbitTemplate.send(SagaTopology.EXCHANGE, routingKey, toMessage(event), correlationData);

		CorrelationData.Confirm confirm = correlationData.getFuture().get(CONFIRM_TIMEOUT_SECONDS, TimeUnit.SECONDS);
		if (!confirm.isAck()) {
			throw new IllegalStateException("브로커가 메시지를 받지 않았습니다(nack): " + confirm.getReason());
		}
		if (correlationData.getReturned() != null) {
			throw new IllegalStateException("메시지가 어떤 큐에도 라우팅되지 않았습니다. routingKey=" + routingKey
					+ ", replyText=" + correlationData.getReturned().getReplyText());
		}
	}

	private Message toMessage(OutboxEvent event) {
		MessageProperties properties = new MessageProperties();
		properties.setContentType(MessageProperties.CONTENT_TYPE_JSON);
		properties.setContentEncoding(StandardCharsets.UTF_8.name());
		properties.setMessageId(event.getEventId());
		properties.setHeader("eventId", event.getEventId());
		properties.setHeader("eventType", event.getEventType());
		return new Message(event.getPayload().getBytes(StandardCharsets.UTF_8), properties);
	}

}
