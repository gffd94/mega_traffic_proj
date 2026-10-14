package com.mega.order.global.idempotency;

/**
 * 이미 처리 기록(processed_events)이 있는 이벤트를 다시 처리하려 할 때 ProcessedEventRecorder가 던진다.
 * 업무 코드보다 먼저 호출되므로 이 예외가 나면 업무는 아무것도 실행되지 않은 상태다.
 * DuplicateEventGuard가 잡아 INFO 로그만 남기고 정상 처리(ack)한다.
 */
public class DuplicateEventException extends RuntimeException {

	public DuplicateEventException(String consumer, String eventId) {
		super("이미 처리된 이벤트입니다. consumer=" + consumer + ", eventId=" + eventId);
	}

}
