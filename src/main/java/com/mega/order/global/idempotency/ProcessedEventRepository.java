package com.mega.order.global.idempotency;

import org.springframework.data.jpa.repository.JpaRepository;

public interface ProcessedEventRepository extends JpaRepository<ProcessedEvent, Long> {

	boolean existsByConsumerAndEventId(String consumer, String eventId);

}
