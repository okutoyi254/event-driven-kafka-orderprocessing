package com.example.DistributedKafkaOrderProcessing.order;

import com.example.DistributedKafkaOrderProcessing.domain.entities.Order;
import com.example.DistributedKafkaOrderProcessing.domain.entities.OrderItem;
import com.example.DistributedKafkaOrderProcessing.domain.enums.OrderStatus;
import com.example.DistributedKafkaOrderProcessing.domain.events.Events;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.math.BigDecimal;
import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;

@Service
public class OrderService {

    private static final Logger log = LoggerFactory.getLogger(OrderService.class);

    private final OrderRepository orderRepository;
    private final OrderEventProducer eventProducer;

    public OrderService(OrderRepository orderRepository, OrderEventProducer eventProducer) {
        this.orderRepository = orderRepository;
        this.eventProducer = eventProducer;
    }

    @Transactional
    public OrderDtos.PlaceOrderResponse placeOrder(OrderDtos.PlaceOrderRequest request) {
        log.info("Placing order for customer: {}", request.customerId());

        // Build items
        List<OrderItem> items = request.items().stream()
                .map(i -> new OrderItem(
                        i.productId(), i.productName(), i.productCategory(),
                        i.quantity(), i.unitPrice()
                ))
                .collect(Collectors.toList());

        // Calculate total
        BigDecimal total = items.stream()
                .map(OrderItem::getSubtotal)
                .reduce(BigDecimal.ZERO, BigDecimal::add);

        // Persist as PENDING
        String orderId = UUID.randomUUID().toString();
        Order order = new Order(
                orderId,
                request.customerId(),
                request.customerEmail(),
                request.customerPhone(),
                items,
                total,
                request.shippingAddress()
        );
        orderRepository.save(order);
        log.info("Order saved as PENDING: orderId={} total={}", orderId, total);

        // Build and publish the event that starts the saga
        Events.OrderPlacedEvent event = new Events.OrderPlacedEvent(
                UUID.randomUUID().toString(),   // eventId — for idempotency
                orderId,
                request.customerId(),
                request.customerEmail(),
                request.customerPhone(),
                request.items().stream()
                        .map(i -> new Events.OrderItem(
                                i.productId(), i.productName(), i.productCategory(),
                                i.quantity(), i.unitPrice(),
                                i.unitPrice().multiply(BigDecimal.valueOf(i.quantity()))
                        ))
                        .toList(),
                total,
                request.shippingAddress(),
                java.time.Instant.now()
        );
        eventProducer.publishOrderPlaced(event);

        return new OrderDtos.PlaceOrderResponse(
                orderId,
                "PENDING",
                total,
                "Order received. Payment processing in progress.",
                "/api/orders/" + orderId
        );
    }

//    Status update called by downtime consumers(saga)
    @Transactional
    public void updateStatus(String orderId, OrderStatus status){

        orderRepository.findById(orderId).ifPresent(
                order -> {
                    order.updateStatus(status);
                    orderRepository.save(order);
                    log.info("Order status updated: orderId={} status={}",orderId,status);
                });
    }

    @Transactional
    public void confirmOrder(String orderId){

        orderRepository.findById(orderId).ifPresent(
                order -> {
                    order.confirm();
                    orderRepository.save(order);
                    log.info("Order CONFIRMED: orderId={}",orderId);
                });

    }

    @Transactional
    public void failOrder(String orderId,String reason){

        orderRepository.findById(orderId).ifPresent(
                order -> {
                    order.fail(reason);
                    orderRepository.save(order);
                    log.info("Order FAILED: orderId={} reason={}", orderId, reason);
                });
    }

    @Transactional(readOnly = true)
    public Order getOrder(String orderId) {
        return orderRepository.findById(orderId)
                .orElseThrow(() -> new OrderNotFoundException(orderId));
    }

    @Transactional(readOnly = true)
    public List<Order> getOrdersByCustomer(String customerId) {
        return orderRepository.findByCustomerIdOrderByPlacedAtDesc(customerId);
    }

    @Transactional(readOnly = true)
    public List<Order> getAllOrders() {
        return orderRepository.findAll();
    }

    public static class OrderNotFoundException extends RuntimeException{

        public OrderNotFoundException(String orderId){
            super("Order not found: "+orderId);
        }
    }
}
