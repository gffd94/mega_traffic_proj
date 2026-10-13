package com.mega.order.global.outbox;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

/**
 * 비즈니스 데이터 변경과 같은 트랜잭션 안에서 Outbox 이벤트를 기록한다.
 * MANDATORY라서 호출한 쪽에 트랜잭션이 없으면 예외가 난다.
 * Outbox 기록만 따로 커밋되거나 비즈니스 데이터만 커밋되는 상황을 막기 위해서다.
 */
@Component
@RequiredArgsConstructor
public class OutboxEventRecorder {

	private final OutboxEventRepository outboxEventRepository;
	private final ObjectMapper objectMapper;

	@Transactional(propagation = Propagation.MANDATORY)
	public void record(AggregateType aggregateType, Long aggregateId, String eventType, Object payload) {
		OutboxEvent outboxEvent = OutboxEvent.builder()
				.aggregateType(aggregateType)
				.aggregateId(aggregateId)
				.eventType(eventType)
				.payload(toJson(payload))
				.build();
		outboxEventRepository.save(outboxEvent);
	}

	private String toJson(Object payload) {
		try {
			return objectMapper.writeValueAsString(payload);
		} catch (JsonProcessingException e) {
			throw new IllegalStateException("Outbox payload를 JSON으로 변환하지 못했습니다: " + payload.getClass().getSimpleName(), e);
		}
	}

}
