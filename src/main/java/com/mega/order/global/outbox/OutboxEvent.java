package com.mega.order.global.outbox;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.EntityListeners;
import jakarta.persistence.EnumType;
import jakarta.persistence.Enumerated;
import jakarta.persistence.GeneratedValue;
import jakarta.persistence.GenerationType;
import jakarta.persistence.Id;
import jakarta.persistence.Index;
import jakarta.persistence.Table;
import java.time.LocalDateTime;
import java.util.UUID;
import lombok.AccessLevel;
import lombok.Builder;
import lombok.Getter;
import lombok.NoArgsConstructor;
import org.springframework.data.annotation.CreatedDate;
import org.springframework.data.jpa.domain.support.AuditingEntityListener;

@Getter
@Entity
@Table(name = "outbox_events", indexes = @Index(name = "idx_outbox_status_created", columnList = "status, created_at"))
@EntityListeners(AuditingEntityListener.class)
@NoArgsConstructor(access = AccessLevel.PROTECTED)
public class OutboxEvent {

	@Id
	@GeneratedValue(strategy = GenerationType.IDENTITY)
	private Long id;

	@Column(nullable = false, unique = true, length = 36)
	private String eventId;

	@Enumerated(EnumType.STRING)
	@Column(nullable = false, columnDefinition = "varchar(30)")
	private AggregateType aggregateType;

	@Column(nullable = false)
	private Long aggregateId;

	@Column(nullable = false, length = 50)
	private String eventType;

	@Column(nullable = false, columnDefinition = "json")
	private String payload;

	@Enumerated(EnumType.STRING)
	@Column(nullable = false, columnDefinition = "varchar(20)")
	private OutboxEventStatus status;

	@CreatedDate
	@Column(nullable = false, updatable = false)
	private LocalDateTime createdAt;

	private LocalDateTime publishedAt;

	@Builder
	private OutboxEvent(AggregateType aggregateType, Long aggregateId, String eventType, String payload) {
		this.eventId = UUID.randomUUID().toString();
		this.aggregateType = aggregateType;
		this.aggregateId = aggregateId;
		this.eventType = eventType;
		this.payload = payload;
		this.status = OutboxEventStatus.READY;
	}

	public void markPublished() {
		if (this.status != OutboxEventStatus.READY) {
			throw new IllegalStateException("현재 상태 " + this.status + "에서는 발행 완료 처리를 할 수 없습니다.");
		}
		this.status = OutboxEventStatus.PUBLISHED;
		this.publishedAt = LocalDateTime.now();
	}

}
