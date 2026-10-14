package com.mega.order.global.idempotency;

import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

/**
 * 컨슈머가 이벤트를 처리했다는 기록(processed_events)을 남긴다.
 *
 * MANDATORY: 업무 변경과 같은 트랜잭션에서만 기록한다. 둘이 같이 커밋되거나 같이 롤백되어야
 * "기록은 남았는데 업무는 반영 안 됨"(재전송이 무시되어 영영 반영 안 됨) 같은 상태가 생기지 않는다.
 *
 * 업무보다 먼저 호출해야 한다. 중복은 두 단계로 막는다.
 * 1. 먼저 조회해서 이미 기록이 있으면 INSERT 없이 DuplicateEventException을 던진다. 재전송처럼 앞선 처리가
 *    이미 커밋된 흔한 중복은 여기서 걸러져, unique 위반(과 Hibernate의 ERROR 로그)까지 가지 않는다.
 * 2. 조회와 INSERT 사이에 같은 이벤트가 동시에 들어온 경우는 unique 제약이 최종으로 막는다.
 *    saveAndFlush로 INSERT를 바로 실행해 자리를 먼저 잡으므로, 뒤에 온 쪽은 업무를 시작하기 전에 멈춘다.
 *    위반 예외는 여기서 잡지 않는다. 트랜잭션 안에서 잡아도 이미 롤백 전용으로 표시되어 커밋할 수 없으므로,
 *    트랜잭션 밖(DuplicateEventGuard)에서 판별한다.
 */
@Component
@RequiredArgsConstructor
public class ProcessedEventRecorder {

	private final ProcessedEventRepository processedEventRepository;

	@Transactional(propagation = Propagation.MANDATORY)
	public void record(String consumer, String eventId) {
		if (processedEventRepository.existsByConsumerAndEventId(consumer, eventId)) {
			throw new DuplicateEventException(consumer, eventId);
		}
		processedEventRepository.saveAndFlush(ProcessedEvent.builder()
				.consumer(consumer)
				.eventId(eventId)
				.build());
	}

}
