package com.mega.order.payment;

import com.mega.order.global.idempotency.ProcessedEventRecorder;
import com.mega.order.global.outbox.AggregateType;
import com.mega.order.global.outbox.OutboxEventRecorder;
import com.mega.order.global.outbox.OutboxEventTypes;
import com.mega.order.payment.event.PaymentFailedEvent;
import com.mega.order.product.event.StockReservedEvent;
import java.util.List;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

/**
 * 결제 실패 기록(payments FAILED)과 PaymentFailed 이벤트를 한 트랜잭션으로 저장한다.
 *
 * 결제 트랜잭션(PaymentService.pay) 안에서 저장하면 포인트 부족 예외로 롤백될 때 이 기록도 같이 사라진다.
 * 그래서 리스너가 예외를 잡은 뒤, 롤백이 끝난 바깥에서 이 클래스를 호출해 새 트랜잭션으로 커밋한다.
 * PaymentService 안의 메서드로 두면 같은 객체 내부 호출이라 프록시를 거치지 않아
 * @Transactional이 적용되지 않으므로 클래스를 나눈다.
 *
 * 결제 트랜잭션과 함께 처리 기록(processed_events)도 롤백됐으므로 여기서 다시 남긴다.
 * 같은 이벤트가 동시에 와서 둘 다 결제에 실패해도, 이 기록의 unique 위반으로 실패 이벤트는 한 번만 저장된다.
 */
@Component
@RequiredArgsConstructor
public class PaymentFailureRecorder {

	private final PaymentRepository paymentRepository;
	private final OutboxEventRecorder outboxEventRecorder;
	private final ProcessedEventRecorder processedEventRecorder;

	// REQUIRES_NEW: 지금은 트랜잭션 없는 리스너에서만 부르지만, 호출한 쪽 트랜잭션과 섞이지 않는다는 의도를 드러낸다
	@Transactional(propagation = Propagation.REQUIRES_NEW)
	public void record(StockReservedEvent event, String reason, String consumer, String eventId) {
		processedEventRecorder.record(consumer, eventId);
		Payment payment = paymentRepository.save(Payment.failed(event.orderId(), event.totalPrice(), reason));

		List<PaymentFailedEvent.Item> items = event.items().stream()
				.map(item -> new PaymentFailedEvent.Item(item.productId(), item.quantity()))
				.toList();
		outboxEventRecorder.record(AggregateType.PAYMENT, payment.getId(), OutboxEventTypes.PAYMENT_FAILED,
				new PaymentFailedEvent(event.orderId(), event.userId(), items, reason));
	}

}
