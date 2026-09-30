package com.mega.order.global.idempotency;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.EntityListeners;
import jakarta.persistence.GeneratedValue;
import jakarta.persistence.GenerationType;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import jakarta.persistence.UniqueConstraint;
import java.time.LocalDateTime;
import lombok.AccessLevel;
import lombok.Builder;
import lombok.Getter;
import lombok.NoArgsConstructor;
import org.springframework.data.annotation.CreatedDate;
import org.springframework.data.jpa.domain.support.AuditingEntityListener;

@Getter
@Entity
@Table(name = "processed_events", uniqueConstraints = @UniqueConstraint(name = "uk_processed_consumer_event", columnNames = {"consumer", "event_id"}))
@EntityListeners(AuditingEntityListener.class)
@NoArgsConstructor(access = AccessLevel.PROTECTED)
public class ProcessedEvent {

	@Id
	@GeneratedValue(strategy = GenerationType.IDENTITY)
	private Long id;

	@Column(nullable = false, length = 50)
	private String consumer;

	@Column(nullable = false, length = 36)
	private String eventId;

	@CreatedDate
	@Column(nullable = false, updatable = false)
	private LocalDateTime processedAt;

	@Builder
	private ProcessedEvent(String consumer, String eventId) {
		this.consumer = consumer;
		this.eventId = eventId;
	}

}
