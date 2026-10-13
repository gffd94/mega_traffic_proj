package com.mega.order.user;

import com.mega.order.global.exception.InsufficientPointException;
import com.mega.order.global.exception.UserNotFoundException;
import java.math.BigDecimal;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

/**
 * 다른 도메인이 user의 상태를 바꿀 수 있는 유일한 창구다. (원칙은 "도메인 간 상태 변경은 이벤트로만")
 *
 * 결제(payment)는 포인트 차감과 결제 기록이 한 트랜잭션으로 묶여야 한다.
 * 차감을 이벤트로 넘기면 "결제는 성공했는데 포인트는 안 빠진" 중간 상태와 그에 대한 보상이 추가로 필요해진다.
 * 그래서 예외를 두되, 범위를 포인트 차감 하나로 좁혀 user 내부 구조(엔티티, 리포지토리)는 노출하지 않는다.
 * 충전·회원 정보 변경 같은 다른 변경은 여기서 열지 않는다.
 * 물리적으로 서버를 나누게 되면 포인트 잔액 자체를 payment 서비스로 옮긴다.
 */
@Component
@RequiredArgsConstructor
public class PointDeductor {

	private final UserRepository userRepository;

	/**
	 * MANDATORY: 호출한 쪽(결제 기록)과 같은 트랜잭션에서만 차감한다. 차감만 따로 커밋되는 것을 막는다.
	 */
	@Transactional(propagation = Propagation.MANDATORY)
	public void deduct(Long userId, BigDecimal amount) {
		if (amount == null || amount.signum() <= 0) {
			throw new IllegalArgumentException("차감 금액은 0보다 커야 합니다. 입력 금액: " + amount);
		}
		int updatedRows = userRepository.decreasePointIfEnough(userId, amount);
		if (updatedRows == 0) {
			// 0행은 회원이 없거나 잔액이 부족한 경우. 다시 조회해서 구분한다.
			User user = userRepository.findById(userId)
					.orElseThrow(() -> new UserNotFoundException(userId));
			throw new InsufficientPointException(amount, user.getPointBalance());
		}
	}

}
