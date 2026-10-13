package com.mega.order.global.config;

import lombok.extern.slf4j.Slf4j;
import org.springframework.amqp.core.Message;
import org.springframework.amqp.rabbit.retry.RejectAndDontRequeueRecoverer;
import org.springframework.stereotype.Component;

/**
 * 컨슈머 재시도(application.yml의 listener.simple.retry)를 모두 소진했을 때 호출된다.
 * 에러 로그를 남긴 뒤 메시지를 reject(requeue=false)한다. 다시 큐에 넣으면 같은 실패가 끝없이 반복되기 때문이다.
 * 지금은 큐에 DLX가 없어서 reject된 메시지는 버려진다.
 * TODO 다음 단계에서 DLQ를 붙이면, 같은 reject로 메시지가 DLQ로 옮겨져 수동 확인할 수 있게 된다.
 */
@Slf4j
@Component
public class SagaMessageRecoverer extends RejectAndDontRequeueRecoverer {

	@Override
	public void recover(Message message, Throwable cause) {
		log.error("메시지 처리 재시도를 모두 실패해 포기합니다. queue={}, eventId={}, eventType={}, 이유={}",
				message.getMessageProperties().getConsumerQueue(),
				message.getMessageProperties().getHeader("eventId"),
				message.getMessageProperties().getHeader("eventType"),
				rootMessage(cause), cause);
		super.recover(message, cause);
	}

	// 리스너 예외는 ListenerExecutionFailedException 등으로 여러 번 감싸져 오므로 실제 원인의 메시지를 꺼낸다
	private String rootMessage(Throwable cause) {
		Throwable root = cause;
		while (root.getCause() != null && root.getCause() != root) {
			root = root.getCause();
		}
		return root.getClass().getSimpleName() + ": " + root.getMessage();
	}

}
