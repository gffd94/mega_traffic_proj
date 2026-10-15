package com.mega.order.global.monitoring;

/**
 * 멱등 컨슈머가 중복 이벤트를 걸러낸 단계 (ProcessedEventRecorder 참고).
 */
public enum DuplicateStage {

	/** 1차: 처리 기록을 조회해서 이미 있다고 확인됨 (앞선 처리가 이미 커밋된 재전송·재배달) */
	LOOKUP("lookup"),

	/** 2차: 조회는 통과했지만 처리 기록 INSERT가 unique 제약에 걸림 (같은 이벤트가 동시에 처리된 경우) */
	UNIQUE("unique");

	private final String tagValue;

	DuplicateStage(String tagValue) {
		this.tagValue = tagValue;
	}

	public String tagValue() {
		return tagValue;
	}

}
