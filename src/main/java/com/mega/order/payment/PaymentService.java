package com.mega.order.payment;

import com.mega.order.global.idempotency.ProcessedEventRecorder;
import com.mega.order.global.outbox.AggregateType;
import com.mega.order.global.outbox.OutboxEventRecorder;
import com.mega.order.global.outbox.OutboxEventTypes;
import com.mega.order.payment.event.PaymentCompletedEvent;
import com.mega.order.product.event.StockReservedEvent;
import com.mega.order.user.PointDeductor;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

@Service
@RequiredArgsConstructor
public class PaymentService {

	private final PaymentRepository paymentRepository;
	private final PointDeductor pointDeductor;
	private final OutboxEventRecorder outboxEventRecorder;
	private final ProcessedEventRecorder processedEventRecorder;

	/**
	 * 처리 기록, 포인트 차감, 결제 기록, PaymentCompleted 기록을 한 트랜잭션으로 처리한다.
	 * 포인트가 부족하면 예외가 나서 넷 다 롤백된다. 처리 기록은 PaymentFailureRecorder가 다시 남긴다.
	 */
	@Transactional
	public void pay(StockReservedEvent event, String consumer, String eventId) {
		processedEventRecorder.record(consumer, eventId);
		pointDeductor.deduct(event.userId(), event.totalPrice());
		Payment payment = paymentRepository.save(Payment.success(event.orderId(), event.totalPrice()));
		outboxEventRecorder.record(AggregateType.PAYMENT, payment.getId(), OutboxEventTypes.PAYMENT_COMPLETED,
				new PaymentCompletedEvent(event.orderId()));
	}

}
