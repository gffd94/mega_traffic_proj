package com.mega.order.global.outbox;

import java.util.List;
import org.springframework.data.jpa.repository.JpaRepository;

public interface OutboxEventRepository extends JpaRepository<OutboxEvent, Long> {

	// created_at이 같은 이벤트끼리도 순서가 고정되도록 id를 보조 정렬 기준으로 쓴다
	List<OutboxEvent> findTop100ByStatusOrderByCreatedAtAscIdAsc(OutboxEventStatus status);

}
