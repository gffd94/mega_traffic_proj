package com.mega.order.global.outbox;

import com.mega.order.global.config.SagaTopology;
import java.util.Map;

/**
 * Outbox eventType 이름과, 각 eventType을 어떤 routing key로 보낼지를 한 곳에서 관리한다.
 * 새 이벤트를 추가할 때는 상수와 매핑을 여기에 같이 추가한다.
 */
public final class OutboxEventTypes {

	public static final String ORDER_CREATED = "OrderCreated";

	private static final Map<String, String> ROUTING_KEY_BY_EVENT_TYPE = Map.of(
			ORDER_CREATED, SagaTopology.ORDER_CREATED_ROUTING_KEY
	);

	private OutboxEventTypes() {
	}

	public static String routingKeyOf(String eventType) {
		String routingKey = ROUTING_KEY_BY_EVENT_TYPE.get(eventType);
		if (routingKey == null) {
			throw new IllegalStateException("routing key가 등록되지 않은 eventType입니다: " + eventType);
		}
		return routingKey;
	}

}
