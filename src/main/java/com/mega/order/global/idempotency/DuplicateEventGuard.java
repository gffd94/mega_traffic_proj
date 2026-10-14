package com.mega.order.global.idempotency;

import lombok.extern.slf4j.Slf4j;
import org.hibernate.exception.ConstraintViolationException;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.stereotype.Component;

/**
 * 리스너(트랜잭션 밖)에서 처리를 감싸, 이미 처리된 이벤트면 INFO 로그만 남기고 정상 종료(ack)시킨다.
 *
 * 중복 판별은 처리 트랜잭션이 롤백된 뒤에 해야 하므로 트랜잭션 없는 리스너에서 호출한다.
 * 중복으로 보는 경우는 두 가지다.
 * - DuplicateEventException: 처리 기록 조회에서 이미 있다고 확인된 경우 (재전송 등 대부분의 중복)
 * - processed_events의 unique 위반: 같은 이벤트가 동시에 들어와 조회를 둘 다 통과한 경우
 * 다른 제약 위반은 데이터 이상일 수 있으므로 그대로 던져 기존 재시도 정책(재시도 후 DLQ)을 따르게 한다.
 */
@Slf4j
@Component
public class DuplicateEventGuard {

	public void run(String consumer, String eventId, Runnable handler) {
		try {
			handler.run();
		} catch (DuplicateEventException e) {
			log.info("이미 처리된 이벤트라 건너뜁니다. consumer={}, eventId={}", consumer, eventId);
		} catch (DataIntegrityViolationException e) {
			if (!isProcessedEventDuplicate(e)) {
				throw e;
			}
			log.info("이미 처리된 이벤트라 건너뜁니다. consumer={}, eventId={}", consumer, eventId);
		}
	}

	// MySQL은 제약 이름을 "processed_events.uk_processed_consumer_event"처럼 테이블 이름을 붙여 알려준다
	private boolean isProcessedEventDuplicate(DataIntegrityViolationException e) {
		for (Throwable cause = e; cause != null; cause = cause.getCause()) {
			if (cause instanceof ConstraintViolationException violation) {
				String constraintName = violation.getConstraintName();
				return constraintName != null
						&& (constraintName.equals(ProcessedEvent.UNIQUE_CONSTRAINT)
						|| constraintName.endsWith("." + ProcessedEvent.UNIQUE_CONSTRAINT));
			}
		}
		return false;
	}

}
