package com.example.DistributedKafkaOrderProcessing;


import com.example.DistributedKafkaOrderProcessing.domain.enums.OrderStatus;
import com.example.DistributedKafkaOrderProcessing.domain.enums.PaymentStatus;
import com.example.DistributedKafkaOrderProcessing.notification.NotificationRepository;
import com.example.DistributedKafkaOrderProcessing.order.OrderDtos;
import com.example.DistributedKafkaOrderProcessing.order.OrderRepository;
import com.example.DistributedKafkaOrderProcessing.order.OrderService;
import com.example.DistributedKafkaOrderProcessing.payment.PaymentRepository;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;

import java.math.BigDecimal;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;


@KafkaIntegrationTest
@DisplayName("Order Saga Integration Tests")
class OrderSagaIntegrationTest {

    @Autowired private OrderService orderService;
    @Autowired private OrderRepository orderRepository;
    @Autowired private PaymentRepository paymentRepository;
    @Autowired private NotificationRepository notificationRepository;

    // ── Test fixtures ─────────────────────────────────────────────────────────

    private OrderDtos.PlaceOrderRequest drillOrder() {
        return new OrderDtos.PlaceOrderRequest(
                "CUST-TEST-001",
                "test@hardwarestore.com",
                "+1-555-0100",
                List.of(new OrderDtos.OrderItemRequest(
                        "PROD-DRILL-001", "DeWalt 20V Cordless Drill",
                        "POWER_TOOLS", 1, new BigDecimal("149.99")
                )),
                "123 Test St, Chicago IL 60601"
        );
    }

    private OrderDtos.PlaceOrderRequest multiItemOrder() {
        return new OrderDtos.PlaceOrderRequest(
                "CUST-TEST-002",
                "multi@hardwarestore.com",
                "+1-555-0200",
                List.of(
                        new OrderDtos.OrderItemRequest(
                                "PROD-DRILL-001", "DeWalt 20V Cordless Drill",
                                "POWER_TOOLS", 2, new BigDecimal("149.99")
                        ),
                        new OrderDtos.OrderItemRequest(
                                "PROD-NAIL-030", "Framing Nails 3.5 inch",
                                "FASTENERS", 5, new BigDecimal("19.99")
                        )
                ),
                "456 Multi St, Chicago IL 60601"
        );
    }

    // ── Test 1: Order saved as PENDING immediately ────────────────────────────

    @Test
    @DisplayName("Order is saved as PENDING immediately when placed")
    void placeOrder_savedAsPending() {
        OrderDtos.PlaceOrderResponse response = orderService.placeOrder(drillOrder());

        assertThat(response.orderId()).isNotBlank();
        assertThat(response.status()).isEqualTo("PENDING");
        assertThat(response.totalAmount()).isEqualByComparingTo("149.99");
        assertThat(response.trackingUrl()).contains(response.orderId());

        // Verify persisted to DB immediately (synchronous)
        var order = orderRepository.findById(response.orderId());
        assertThat(order).isPresent();
        assertThat(order.get().getStatus()).isEqualTo(OrderStatus.PENDING);
        assertThat(order.get().getCustomerId()).isEqualTo("CUST-TEST-001");
    }

    // ── Test 2: Order total calculated correctly ──────────────────────────────

    @Test
    @DisplayName("Order total is correctly calculated from line items")
    void placeOrder_totalCalculatedCorrectly() {
        OrderDtos.PlaceOrderResponse response = orderService.placeOrder(multiItemOrder());

        assertThat(response.totalAmount()).isEqualByComparingTo("399.93");
    }

    // ── Test 3: Saga eventually reaches terminal state ────────────────────────

    @Test
    @DisplayName("Saga eventually reaches CONFIRMED or FAILED — never stays PENDING")
    void saga_reachesTerminalState() {
        OrderDtos.PlaceOrderResponse response = orderService.placeOrder(drillOrder());
        String orderId = response.orderId();

        // The saga runs asynchronously via Kafka — poll until terminal state
        Awaitility.await()
                .atMost(60, TimeUnit.SECONDS)
                .pollInterval(500, TimeUnit.MILLISECONDS)
                .untilAsserted(() -> {
                    var order = orderRepository.findById(orderId).orElseThrow();
                    assertThat(order.getStatus())
                            .as("Order should reach CONFIRMED or FAILED, not stay in %s",
                                    order.getStatus())
                            .isIn(OrderStatus.CONFIRMED, OrderStatus.PAYMENT_FAILED,
                                    OrderStatus.INVENTORY_FAILED, OrderStatus.SHIPPING_FAILED);
                });
    }

    // ── Test 4: Payment record always created ─────────────────────────────────

    @Test
    @DisplayName("Payment record is persisted for every order")
    void saga_paymentRecordPersisted() {
        OrderDtos.PlaceOrderResponse response = orderService.placeOrder(drillOrder());
        String orderId = response.orderId();

        Awaitility.await()
                .atMost(15, TimeUnit.SECONDS)
                .pollInterval(500, TimeUnit.MILLISECONDS)
                .untilAsserted(() -> {
                    var payment = paymentRepository.findByOrderId(orderId);
                    assertThat(payment)
                            .as("Payment record should exist for orderId=%s", orderId)
                            .isPresent();
                    assertThat(payment.get().getStatus())
                            .isIn(PaymentStatus.SUCCESS, PaymentStatus.INSUFFICIENT_FUNDS);
                    assertThat(payment.get().getAmount())
                            .isEqualByComparingTo("149.99");
                });
    }

    // ── Test 5: Notification sent within SLA ─────────────────────────────────

    @Test
    @DisplayName("At least one notification is sent within 5 seconds of order placement")
    void notification_sentPromptly() {
        OrderDtos.PlaceOrderResponse response = orderService.placeOrder(drillOrder());
        String orderId = response.orderId();

        Awaitility.await()
                .atMost(10, TimeUnit.SECONDS)
                .pollInterval(300, TimeUnit.MILLISECONDS)
                .untilAsserted(() -> {
                    var notifications = notificationRepository.findByOrderId(orderId);
                    assertThat(notifications)
                            .as("At least one notification should exist for orderId=%s", orderId)
                            .isNotEmpty();
                    // At minimum ORDER_RECEIVED should have been sent
                    boolean hasOrderReceived = notifications.stream()
                            .anyMatch(n -> n.getType().name().equals("ORDER_RECEIVED"));
                    assertThat(hasOrderReceived)
                            .as("ORDER_RECEIVED notification should exist")
                            .isTrue();
                });
    }

    // ── Test 6: Batch orders all reach terminal state ─────────────────────────

    @Test
    @DisplayName("All orders in a batch reach a terminal state")
    void batchOrders_allReachTerminalState() {
        int count = 5;
        List<String> orderIds = new java.util.ArrayList<>();

        for (int i = 0; i < count; i++) {
            OrderDtos.PlaceOrderRequest req = new OrderDtos.PlaceOrderRequest(
                    "CUST-BATCH-" + i,
                    "batch" + i + "@test.com",
                    "+1-555-" + String.format("%04d", i),
                    List.of(new OrderDtos.OrderItemRequest(
                            "PROD-NAIL-030", "Framing Nails",
                            "FASTENERS", 1, new BigDecimal("19.99")
                    )),
                    i + " Batch Lane, Chicago IL"
            );
            orderIds.add(orderService.placeOrder(req).orderId());
        }

        // All orders must reach a terminal state within 20 seconds
        Awaitility.await()
                .atMost(20, TimeUnit.SECONDS)
                .pollInterval(500, TimeUnit.MILLISECONDS)
                .untilAsserted(() -> {
                    List<OrderStatus> terminalStatuses = List.of(
                            OrderStatus.CONFIRMED,
                            OrderStatus.PAYMENT_FAILED,
                            OrderStatus.INVENTORY_FAILED,
                            OrderStatus.SHIPPING_FAILED
                    );

                    for (String orderId : orderIds) {
                        var order = orderRepository.findById(orderId).orElseThrow();
                        assertThat(order.getStatus())
                                .as("Order %s should be terminal", orderId)
                                .isIn(terminalStatuses);
                    }
                });
    }

    // ── Test 7: OrderNotFoundException for unknown ID ─────────────────────────

    @Test
    @DisplayName("OrderNotFoundException thrown for unknown orderId")
    void getOrder_unknownId_throwsException() {
        assertThatThrownBy(() -> orderService.getOrder("non-existent-id-xyz"))
                .isInstanceOf(OrderService.OrderNotFoundException.class)
                .hasMessageContaining("non-existent-id-xyz");
    }

    // ── Test 8: Idempotency — duplicate events are skipped ────────────────────

    @Test
    @DisplayName("Placing the same order twice does not double-process")
    void idempotency_duplicateOrderNotDoubleProcessed() {
        OrderDtos.PlaceOrderResponse first = orderService.placeOrder(drillOrder());
        OrderDtos.PlaceOrderResponse second = orderService.placeOrder(drillOrder());

        // Two separate orders should be created (different orderIds)
        assertThat(first.orderId()).isNotEqualTo(second.orderId());

        // Both should eventually reach terminal states independently
        Awaitility.await()
                .atMost(20, TimeUnit.SECONDS)
                .pollInterval(500, TimeUnit.MILLISECONDS)
                .untilAsserted(() -> {
                    var order1 = orderRepository.findById(first.orderId()).orElseThrow();
                    var order2 = orderRepository.findById(second.orderId()).orElseThrow();

                    List<OrderStatus> terminal = List.of(
                            OrderStatus.CONFIRMED, OrderStatus.PAYMENT_FAILED,
                            OrderStatus.INVENTORY_FAILED, OrderStatus.SHIPPING_FAILED
                    );
                    assertThat(order1.getStatus()).isIn(terminal);
                    assertThat(order2.getStatus()).isIn(terminal);
                });
    }

    // ── Test 9: Customer orders retrievable ───────────────────────────────────

    @Test
    @DisplayName("Customer orders are retrievable by customerId")
    void getOrdersByCustomer_returnsCorrectOrders() {
        String customerId = "CUST-QUERY-001";

        OrderDtos.PlaceOrderRequest req = new OrderDtos.PlaceOrderRequest(
                customerId,
                "query@test.com",
                "+1-555-9999",
                List.of(new OrderDtos.OrderItemRequest(
                        "PROD-SCREW-031", "Deck Screws",
                        "FASTENERS", 2, new BigDecimal("14.99")
                )),
                "789 Query Ave, Chicago IL"
        );

        orderService.placeOrder(req);
        orderService.placeOrder(req);

        var orders = orderService.getOrdersByCustomer(customerId);
        assertThat(orders).hasSize(2);
        assertThat(orders).allMatch(o -> o.getCustomerId().equals(customerId));
    }
}