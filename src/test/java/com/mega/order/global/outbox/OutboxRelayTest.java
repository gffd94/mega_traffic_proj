package com.mega.order.global.outbox;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.mega.order.global.monitoring.SagaMetrics;
import com.mega.order.order.OrderRepository;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.time.Duration;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.LongStream;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.springframework.amqp.AmqpException;
import org.springframework.amqp.core.Message;
import org.springframework.amqp.core.ReturnedMessage;
import org.springframework.amqp.rabbit.connection.CorrelationData;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.test.util.ReflectionTestUtils;

class OutboxRelayTest {

	// 시간 초과 경우를 빠르게 확인하려고 운영값(5초) 대신 짧게 둔다
	private static final Duration CONFIRM_TIMEOUT = Duration.ofMillis(300);

	private enum Broker { ACK, NACK, RETURNED, NO_CONFIRM, SEND_FAILS }

	private OutboxEventRepository outboxEventRepository;
	private RabbitTemplate rabbitTemplate;
	private SimpleMeterRegistry registry;
	private OutboxRelay relay;

	// eventId별로 브로커가 어떻게 응답할지 정한다. 정하지 않은 이벤트는 ack
	private final Map<String, Broker> brokerBehavior = new HashMap<>();

	@BeforeEach
	void setUp() {
		outboxEventRepository = mock(OutboxEventRepository.class);
		rabbitTemplate = mock(RabbitTemplate.class);
		registry = new SimpleMeterRegistry();
		SagaMetrics sagaMetrics = new SagaMetrics(registry, mock(OrderRepository.class), outboxEventRepository);
		relay = new OutboxRelay(outboxEventRepository, rabbitTemplate, sagaMetrics, CONFIRM_TIMEOUT);

		doAnswer(invocation -> {
			Message message = invocation.getArgument(2);
			CorrelationData correlationData = invocation.getArgument(3);
			switch (brokerBehavior.getOrDefault(correlationData.getId(), Broker.ACK)) {
				case ACK -> correlationData.getFuture().complete(new CorrelationData.Confirm(true, null));
				case NACK -> correlationData.getFuture().complete(new CorrelationData.Confirm(false, "test nack"));
				case RETURNED -> {
					// 실제 브로커처럼 return이 먼저 오고, 브로커는 받았으므로 ack가 온다
					correlationData.setReturned(new ReturnedMessage(message, 312, "NO_ROUTE", invocation.getArgument(0), invocation.getArgument(1)));
					correlationData.getFuture().complete(new CorrelationData.Confirm(true, null));
				}
				case NO_CONFIRM -> {
				}
				case SEND_FAILS -> throw new AmqpException("test send failure");
			}
			return null;
		}).when(rabbitTemplate).send(anyString(), anyString(), any(Message.class), any(CorrelationData.class));
	}

	@Test
	void 모두_ack를_받으면_모두_PUBLISHED로_표시한다() {
		List<OutboxEvent> events = events(1, 3);
		when(outboxEventRepository.findTop100ByStatusOrderByCreatedAtAscIdAsc(OutboxEventStatus.READY)).thenReturn(events);

		relay.relay();

		assertThat(markedPublishedIds()).containsExactly(1L, 2L, 3L);
		assertThat(registry.counter("mega.outbox.relay.published").count()).isEqualTo(3);
		assertThat(registry.summary("mega.outbox.relay.batch.size").totalAmount()).isEqualTo(3);
		assertThat(registry.timer("mega.outbox.relay.batch.duration").count()).isEqualTo(1);
	}

	@Test
	void nack_반환_시간초과_전송실패인_이벤트만_READY로_남는다() {
		List<OutboxEvent> events = events(1, 6);
		brokerBehavior.put(eventId(events, 2), Broker.NACK);
		brokerBehavior.put(eventId(events, 3), Broker.RETURNED);
		brokerBehavior.put(eventId(events, 4), Broker.NO_CONFIRM);
		brokerBehavior.put(eventId(events, 6), Broker.SEND_FAILS);
		when(outboxEventRepository.findTop100ByStatusOrderByCreatedAtAscIdAsc(OutboxEventStatus.READY)).thenReturn(events);

		long startedAt = System.nanoTime();
		relay.relay();
		Duration elapsed = Duration.ofNanos(System.nanoTime() - startedAt);

		// 2(nack), 3(반환), 4(시간 초과), 6(전송 실패)은 UPDATE 대상에서 빠져 READY로 남는다
		assertThat(markedPublishedIds()).containsExactly(1L, 5L);
		assertThat(registry.counter("mega.outbox.relay.published").count()).isEqualTo(2);
		assertThat(registry.counter("mega.outbox.relay.failed", "reason", "confirm_failed").count()).isEqualTo(3);
		assertThat(registry.counter("mega.outbox.relay.failed", "reason", "returned").count()).isEqualTo(1);
		// 시간 초과는 배치 전체에 한 번만 적용된다 (이벤트마다 기다리지 않음)
		assertThat(elapsed).isLessThan(CONFIRM_TIMEOUT.multipliedBy(2));
	}

	@Test
	void 확인된_이벤트가_없으면_UPDATE하지_않는다() {
		List<OutboxEvent> events = events(1, 2);
		brokerBehavior.put(eventId(events, 1), Broker.NACK);
		brokerBehavior.put(eventId(events, 2), Broker.NO_CONFIRM);
		when(outboxEventRepository.findTop100ByStatusOrderByCreatedAtAscIdAsc(OutboxEventStatus.READY)).thenReturn(events);

		relay.relay();

		verify(outboxEventRepository, never()).markPublished(anyList(), any());
	}

	@Test
	void 배치가_꽉_차면_쉬지_않고_바로_다음_배치를_조회한다() {
		when(outboxEventRepository.findTop100ByStatusOrderByCreatedAtAscIdAsc(OutboxEventStatus.READY))
				.thenReturn(events(1, OutboxRelay.BATCH_SIZE))
				.thenReturn(events(101, 2));

		relay.relay();

		// 한 번의 relay() 호출 안에서 두 번 조회했다 (스케줄러의 다음 주기를 기다리지 않음)
		verify(outboxEventRepository, times(2)).findTop100ByStatusOrderByCreatedAtAscIdAsc(OutboxEventStatus.READY);
		verify(outboxEventRepository, times(2)).markPublished(anyList(), any());
		assertThat(registry.counter("mega.outbox.relay.published").count()).isEqualTo(OutboxRelay.BATCH_SIZE + 2);
	}

	@Test
	void 배치가_덜_차면_이번_실행을_끝낸다() {
		when(outboxEventRepository.findTop100ByStatusOrderByCreatedAtAscIdAsc(OutboxEventStatus.READY))
				.thenReturn(events(1, OutboxRelay.BATCH_SIZE - 1));

		relay.relay();

		verify(outboxEventRepository, times(1)).findTop100ByStatusOrderByCreatedAtAscIdAsc(OutboxEventStatus.READY);
	}

	@Test
	void 꽉_찬_배치가_계속_나와도_한_번_실행에서는_상한만큼만_처리한다() {
		when(outboxEventRepository.findTop100ByStatusOrderByCreatedAtAscIdAsc(OutboxEventStatus.READY))
				.thenAnswer(invocation -> events(1, OutboxRelay.BATCH_SIZE));

		relay.relay();

		verify(outboxEventRepository, times(OutboxRelay.MAX_BATCHES_PER_RUN))
				.findTop100ByStatusOrderByCreatedAtAscIdAsc(OutboxEventStatus.READY);
	}

	@Test
	void 가져온_이벤트가_없으면_전송도_지표_기록도_하지_않는다() {
		when(outboxEventRepository.findTop100ByStatusOrderByCreatedAtAscIdAsc(OutboxEventStatus.READY)).thenReturn(List.of());

		relay.relay();

		verify(rabbitTemplate, never()).send(anyString(), anyString(), any(Message.class), any(CorrelationData.class));
		verify(outboxEventRepository, never()).markPublished(anyList(), any());
		assertThat(registry.summary("mega.outbox.relay.batch.size").count()).isZero();
	}

	@SuppressWarnings("unchecked")
	private List<Long> markedPublishedIds() {
		ArgumentCaptor<List<Long>> ids = ArgumentCaptor.forClass(List.class);
		verify(outboxEventRepository).markPublished(ids.capture(), any(LocalDateTime.class));
		return ids.getValue();
	}

	private List<OutboxEvent> events(long firstId, int count) {
		List<OutboxEvent> events = new ArrayList<>();
		LongStream.range(firstId, firstId + count).forEach(id -> {
			OutboxEvent event = OutboxEvent.builder()
					.aggregateType(AggregateType.ORDER)
					.aggregateId(id)
					.eventType(OutboxEventTypes.ORDER_CREATED)
					.payload("{\"orderId\":" + id + "}")
					.build();
			ReflectionTestUtils.setField(event, "id", id);
			events.add(event);
		});
		return events;
	}

	private String eventId(List<OutboxEvent> events, long id) {
		return events.stream().filter(e -> e.getId() == id).findFirst().orElseThrow().getEventId();
	}

}
