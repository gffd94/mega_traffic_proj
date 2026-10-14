package com.mega.order.global.outbox;

import java.time.LocalDateTime;
import java.util.List;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Modifying;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;
import org.springframework.transaction.annotation.Transactional;

public interface OutboxEventRepository extends JpaRepository<OutboxEvent, Long> {

	// created_at이 같은 이벤트끼리도 순서가 고정되도록 id를 보조 정렬 기준으로 쓴다.
	// 가져오는 개수(100)는 OutboxRelay.BATCH_SIZE와 같아야 한다 (꽉 찬 배치인지 판단하는 기준)
	List<OutboxEvent> findTop100ByStatusOrderByCreatedAtAscIdAsc(OutboxEventStatus status);

	long countByStatus(OutboxEventStatus status);

	// READY인 것만 바꾼다. 이미 PUBLISHED인 이벤트의 published_at을 덮어쓰지 않기 위해서다
	@Transactional
	@Modifying
	@Query("UPDATE OutboxEvent e SET e.status = com.mega.order.global.outbox.OutboxEventStatus.PUBLISHED, e.publishedAt = :publishedAt "
			+ "WHERE e.id IN :ids AND e.status = com.mega.order.global.outbox.OutboxEventStatus.READY")
	int markPublished(@Param("ids") List<Long> ids, @Param("publishedAt") LocalDateTime publishedAt);

}
