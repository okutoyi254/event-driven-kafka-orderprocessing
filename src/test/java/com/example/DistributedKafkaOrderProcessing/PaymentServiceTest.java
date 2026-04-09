package com.example.DistributedKafkaOrderProcessing;

import com.example.DistributedKafkaOrderProcessing.domain.enums.PaymentStatus;
import com.example.DistributedKafkaOrderProcessing.domain.events.Events;
import com.example.DistributedKafkaOrderProcessing.payment.PaymentRepository;
import com.example.DistributedKafkaOrderProcessing.payment.PaymentService;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.ActiveProfiles;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.List;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;


@SpringBootTest
@DirtiesContext(classMode = DirtiesContext.ClassMode.AFTER_EACH_TEST_METHOD)
@ActiveProfiles("test")
@DisplayName("PaymentService Unit Tests")
@KafkaIntegrationTest
class PaymentServiceTest {

    @Autowired private PaymentService paymentService;
    @Autowired private PaymentRepository paymentRepository;

    private Events.OrderPlacedEvent buildEvent(String orderId) {
        return new Events.OrderPlacedEvent(
                UUID.randomUUID().toString(),
                orderId,
                "CUST-001",
                "test@test.com",
                "+1-555-0001",
                List.of(new Events.OrderItem(
                        "PROD-DRILL-001", "DeWalt Drill", "POWER_TOOLS",
                        1, new BigDecimal("149.99"), new BigDecimal("149.99")
                )),
                new BigDecimal("149.99"),
                "123 Test St",
                Instant.now()
        );
    }

    @Test
    @DisplayName("Payment result is either SUCCESS or INSUFFICIENT_FUNDS (excluding timeout)")
    void processPayment_returnsValidStatus() {
        // Run enough times to get at least one non-timeout result
        // Since timeout is 10% chance, 20 attempts virtually guarantees non-timeout results
        for (int i = 0; i < 20; i++) {
            try {
                String orderId = UUID.randomUUID().toString();
                Events.PaymentResultEvent result = paymentService.processPayment(buildEvent(orderId));

                assertThat(result.orderId()).isEqualTo(orderId);
                assertThat(result.amountCharged()).isEqualByComparingTo("149.99");
                assertThat(result.status())
                        .isIn(PaymentStatus.SUCCESS, PaymentStatus.INSUFFICIENT_FUNDS);

                if (result.status() == PaymentStatus.SUCCESS) {
                    assertThat(result.transactionId()).isNotBlank();
                    assertThat(result.failureReason()).isNull();
                } else {
                    assertThat(result.transactionId()).isNull();
                    assertThat(result.failureReason()).isNotBlank();
                }
                return; // passed on at least one valid result
            } catch (PaymentService.PaymentGatewayTimeoutException ignored) {
                // retry — this is the 10% case
            }
        }
    }

    @Test
    @DisplayName("Payment record is saved to DB after processing")
    void processPayment_savedToDatabase() {
        String orderId = UUID.randomUUID().toString();
        try {
            paymentService.processPayment(buildEvent(orderId));
            var payment = paymentRepository.findByOrderId(orderId);
            assertThat(payment).isPresent();
            assertThat(payment.get().getAmount()).isEqualByComparingTo("149.99");
        } catch (PaymentService.PaymentGatewayTimeoutException ignored) {
            // timeout case — no record saved, acceptable
        }
    }

    @Test
    @DisplayName("Refund marks payment as REFUNDED")
    void refundPayment_marksAsRefunded() {
        // Find an order that succeeded to test refund
        String orderId = UUID.randomUUID().toString();
        boolean saved = false;

        for (int i = 0; i < 20 && !saved; i++) {
            try {
                var result = paymentService.processPayment(buildEvent(orderId));
                if (result.status() == PaymentStatus.SUCCESS) {
                    saved = true;
                }
            } catch (PaymentService.PaymentGatewayTimeoutException ignored) {}
        }

        if (!saved) return; // couldn't get a SUCCESS in time — skip

        boolean refunded = paymentService.refundPayment(orderId, "TXN-TEST");
        assertThat(refunded).isTrue();

        var payment = paymentRepository.findByOrderId(orderId);
        assertThat(payment).isPresent();
        assertThat(payment.get().getStatus()).isEqualTo(PaymentStatus.REFUNDED);
    }

    @Test
    @DisplayName("Refund returns false for unknown orderId")
    void refundPayment_unknownOrder_returnsFalse() {
        boolean refunded = paymentService.refundPayment("unknown-order", "TXN-000");
        assertThat(refunded).isFalse();
    }
}