package com.mega.order.global.outbox;

import com.mega.order.global.config.SagaTopology;
import com.mega.order.global.monitoring.SagaMetrics;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import lombok.extern.slf4j.Slf4j;
import org.springframework.amqp.core.Message;
import org.springframework.amqp.core.MessageProperties;
import org.springframework.amqp.rabbit.connection.CorrelationData;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

/**
 * READY 상태의 Outbox 이벤트를 RabbitMQ에 전송하고 PUBLISHED로 바꾼다.
 *
 * 전송과 PUBLISHED 표시는 하나의 트랜잭션으로 묶을 수 없다 (RabbitMQ와 DB는 별개 시스템).
 * 그래서 전송에 성공한 뒤 PUBLISHED 표시가 커밋되기 전에 서버가 죽으면, 이벤트는 READY로 남아
 * 재시작 후 같은 이벤트가 한 번 더 전송된다 (at-least-once).
 * 이 중복은 나중에 컨슈머가 eventId 헤더로 processed_events를 확인하는 멱등성 처리로 걸러낸다.
 *
 * 반대로 PUBLISHED로 먼저 바꾸고 전송하면, 그 사이에 죽었을 때 이벤트가 유실된다.
 * 유실은 되돌릴 방법이 없고 중복은 컨슈머에서 걸러낼 수 있으므로 "전송 후 표시" 순서를 택한다.
 *
 * 한 배치(최대 BATCH_SIZE건)는 다음 순서로 처리한다.
 * 1. 모두 전송한다 (confirm을 기다리지 않음)
 * 2. 각 이벤트의 confirm을 모아서 기다린다 (배치 전체 상한 confirmTimeout)
 * 3. ack를 받았고 반환되지 않은 이벤트만 UPDATE 한 번으로 PUBLISHED 표시한다
 * nack, 반환, 시간 초과, 전송 예외인 이벤트는 READY로 남아 다음 조회에서 다시 전송된다.
 * 한 통씩 confirm을 기다리면 브로커 왕복 시간이 이벤트 수만큼 쌓이지만, 모아서 기다리면 배치당 한 번에 가깝게 줄어든다.
 *
 * 같은 주문의 이벤트가 한 배치에 함께 들어오는 경우는 앞 이벤트가 이미 전달됐는데 READY로 남은 경우뿐이다
 * (confirm 시간 초과, PUBLISHED 표시 전 서버 종료 등). 다음 이벤트는 앞 이벤트를 소비하는 트랜잭션에서만 기록되기 때문이다.
 * 이때 앞 이벤트는 중복 전송이 되어 컨슈머의 멱등성 처리로 걸러진다.
 *
 * 서버 1대를 가정한다. 여러 대가 동시에 돌면 같은 READY 이벤트를 함께 가져가 중복 전송이 늘어난다.
 */
@Slf4j
@Component
public class OutboxRelay {

	static final int BATCH_SIZE = 100;

	// 꽉 찬 배치가 계속 나와도 한 번 깨어났을 때 이만큼만 처리하고 스케줄러에 돌려준다.
	// 스케줄러 스레드가 하나라서, 무한히 돌면 다른 @Scheduled 작업이 실행되지 못한다.
	static final int MAX_BATCHES_PER_RUN = 50;

	private final OutboxEventRepository outboxEventRepository;
	private final RabbitTemplate rabbitTemplate;
	private final SagaMetrics sagaMetrics;
	private final Duration confirmTimeout;

	public OutboxRelay(OutboxEventRepository outboxEventRepository, RabbitTemplate rabbitTemplate, SagaMetrics sagaMetrics,
			@Value("${mega.outbox.relay.confirm-timeout:5s}") Duration confirmTimeout) {
		this.outboxEventRepository = outboxEventRepository;
		this.rabbitTemplate = rabbitTemplate;
		this.sagaMetrics = sagaMetrics;
		this.confirmTimeout = confirmTimeout;
	}

	// fixedDelay: 이전 실행이 끝나고 idle-delay-ms 뒤에 다음 실행. 실행이 겹치지 않는다.
	// 꽉 찬 배치는 relay() 안에서 쉬지 않고 이어서 처리하므로, 여기서 쉬는 것은 READY가 바닥났을 때뿐이다.
	@Scheduled(fixedDelayString = "${mega.outbox.relay.idle-delay-ms:100}")
	public void relay() {
		for (int i = 0; i < MAX_BATCHES_PER_RUN; i++) {
			int fetched = relayBatch();
			if (fetched < BATCH_SIZE) {
				return;
			}
		}
	}

	/** 한 배치를 처리하고 조회한 이벤트 수를 돌려준다. */
	int relayBatch() {
		long startedAt = System.nanoTime();
		List<OutboxEvent> events = outboxEventRepository.findTop100ByStatusOrderByCreatedAtAscIdAsc(OutboxEventStatus.READY);
		if (events.isEmpty()) {
			return 0;
		}

		List<PendingConfirm> pendings = sendAll(events);
		List<Long> confirmedIds = awaitConfirms(pendings);
		if (!confirmedIds.isEmpty()) {
			// 이 UPDATE가 실패하면 예외가 스케줄러까지 올라가 배치 전체가 READY로 남고 다음 실행에서 다시 전송된다 (중복 전송)
			outboxEventRepository.markPublished(confirmedIds, LocalDateTime.now());
		}

		sagaMetrics.relayBatch(events.size(), Duration.ofNanos(System.nanoTime() - startedAt));
		return events.size();
	}

	private List<PendingConfirm> sendAll(List<OutboxEvent> events) {
		List<PendingConfirm> pendings = new ArrayList<>(events.size());
		for (OutboxEvent event : events) {
			String routingKey = OutboxEventTypes.routingKeyOf(event.getEventType());
			CorrelationData correlationData = new CorrelationData(event.getEventId());
			try {
				rabbitTemplate.send(SagaTopology.EXCHANGE, routingKey, toMessage(event), correlationData);
			} catch (Exception e) {
				// 이 이벤트는 READY로 남아 다음 조회에서 다시 시도된다. 나머지 이벤트는 계속 전송한다
				sagaMetrics.relayConfirmFailed();
				log.warn("Outbox 이벤트 전송 실패. eventId={}, eventType={}", event.getEventId(), event.getEventType(), e);
				continue;
			}
			pendings.add(new PendingConfirm(event, routingKey, correlationData));
		}
		return pendings;
	}

	/**
	 * publisher confirm으로 브로커가 메시지를 받았는지, mandatory return으로 큐까지 라우팅됐는지 확인한다.
	 * (confirm 없이 send만 하면 브로커가 메시지를 받지 못했거나 버려도 성공처럼 보인다)
	 * 대기 상한은 배치 전체에 한 번 적용한다. 앞 이벤트를 기다리는 동안 뒤 이벤트의 confirm도 함께 도착한다.
	 */
	private List<Long> awaitConfirms(List<PendingConfirm> pendings) {
		long deadline = System.nanoTime() + confirmTimeout.toNanos();
		List<Long> confirmedIds = new ArrayList<>(pendings.size());
		for (PendingConfirm pending : pendings) {
			OutboxEvent event = pending.event();
			CorrelationData correlationData = pending.correlationData();
			CorrelationData.Confirm confirm;
			try {
				long remaining = Math.max(0, deadline - System.nanoTime());
				confirm = correlationData.getFuture().get(remaining, TimeUnit.NANOSECONDS);
			} catch (InterruptedException e) {
				Thread.currentThread().interrupt();
				sagaMetrics.relayConfirmFailed();
				return confirmedIds;
			} catch (TimeoutException e) {
				sagaMetrics.relayConfirmFailed();
				log.warn("confirm 대기 시간을 넘겼습니다. eventId={}, eventType={}", event.getEventId(), event.getEventType());
				continue;
			} catch (Exception e) {
				sagaMetrics.relayConfirmFailed();
				log.warn("confirm을 받지 못했습니다. eventId={}, eventType={}", event.getEventId(), event.getEventType(), e);
				continue;
			}

			if (!confirm.isAck()) {
				sagaMetrics.relayConfirmFailed();
				log.warn("브로커가 메시지를 받지 않았습니다(nack). eventId={}, eventType={}, reason={}",
						event.getEventId(), event.getEventType(), confirm.getReason());
				continue;
			}
			// 반환(return)은 confirm보다 먼저 도착하고, Spring AMQP는 future를 완료하기 전에 returned를 채워 둔다
			if (correlationData.getReturned() != null) {
				sagaMetrics.relayReturned();
				log.warn("메시지가 어떤 큐에도 라우팅되지 않았습니다. eventId={}, routingKey={}, replyText={}",
						event.getEventId(), pending.routingKey(), correlationData.getReturned().getReplyText());
				continue;
			}
			sagaMetrics.relayPublished();
			confirmedIds.add(event.getId());
		}
		return confirmedIds;
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

	private record PendingConfirm(OutboxEvent event, String routingKey, CorrelationData correlationData) {
	}

}
