package com.example.DistributedKafkaOrderProcessing;

import com.example.DistributedKafkaOrderProcessing.domain.enums.OrderStatus;
import com.example.DistributedKafkaOrderProcessing.notification.NotificationRepository;
import com.example.DistributedKafkaOrderProcessing.order.OrderDtos;
import com.example.DistributedKafkaOrderProcessing.order.OrderRepository;
import com.example.DistributedKafkaOrderProcessing.order.OrderService;
import com.example.DistributedKafkaOrderProcessing.payment.PaymentRepository;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;

import java.math.BigDecimal;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

@KafkaIntegrationTest
@DisplayName("Order saga Integration Tests")
public class OrderSagaIntegrationTest {

    @Autowired private OrderService orderService;
    @Autowired private OrderRepository orderRepository;
    @Autowired private PaymentRepository paymentRepository;
    @Autowired private NotificationRepository notificationRepository;

//    Tests
    private OrderDtos.PlaceOrderRequest drillOrder(){

        return new OrderDtos.PlaceOrderRequest(
                "CUST-TEST-001",
                "test@hardwarestore.com",
                "+25470707636849",
                List.of(new OrderDtos.OrderItemRequest(
                        "PROD-DRILL-001","DeWalt 20V Cordless Drill","POWER_TOOLS", 1, new BigDecimal("149.99")
                )),
                "123 Test St,Nairobi 1L 60601"
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

//    Test 1: Order saved as PENDING immediately
    @Test
    @DisplayName("Order is saved as PENDING immediately when placed")
    void placeOrder_savedAsPending(){

        OrderDtos.PlaceOrderResponse  response= orderService.placeOrder(drillOrder());

        assertThat(response.orderId()).isNotNull();
        assertThat(response.status()).isEqualTo("PENDING");
        assertThat(response.totalAmount()).isEqualByComparingTo(new BigDecimal("149.99"));
        assertThat(response.trackingUrl()).contains(response.orderId());

        var order = orderRepository.findById(response.orderId());
        assertThat(order).isPresent();
        assertThat(order.get().getStatus()).isEqualTo(OrderStatus.PENDING);
        assertThat(order.get().getCustomerId()).isEqualTo("CUST-TEST-001");
    }

//    Test 2: Order calculates total correctly
    @Test
    @DisplayName("Order calculates total amount correctly")
    void placeOrder_calculatesTotal(){

        OrderDtos.PlaceOrderResponse response = orderService.placeOrder(multiItemOrder());

        assertThat(response.totalAmount()).isEqualByComparingTo(new BigDecimal("399.93"));
    }


}
