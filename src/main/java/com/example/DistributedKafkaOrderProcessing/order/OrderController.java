package com.example.DistributedKafkaOrderProcessing.order;



import com.example.DistributedKafkaOrderProcessing.domain.entities.Order;
import jakarta.validation.Valid;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.math.BigDecimal;
import java.util.List;
import java.util.Map;

@RestController
@RequestMapping("/api/orders")
public class OrderController {

    private static final Logger log = LoggerFactory.getLogger(OrderController.class);

    private final OrderService orderService;

    public OrderController(OrderService orderService) {
        this.orderService = orderService;
    }

    // ── POST /api/orders ──────────────────────────────────────────────────────

    /**
     * Place a new order — starts the Saga.
     *
     * Returns immediately with PENDING status.
     * The saga (payment → inventory → shipping) runs asynchronously via Kafka.
     * Poll GET /api/orders/{orderId} to check final status.
     */
    @PostMapping
    public ResponseEntity<OrderDtos.PlaceOrderResponse> placeOrder(
            @Valid @RequestBody OrderDtos.PlaceOrderRequest request) {
        log.info("POST /api/orders — customerId={} items={}",
                request.customerId(), request.items().size());
        OrderDtos.PlaceOrderResponse response = orderService.placeOrder(request);
        return ResponseEntity.status(HttpStatus.ACCEPTED).body(response);
        // 202 Accepted — not 201 Created — because the order isn't fully
        // confirmed yet. The saga is running asynchronously.
    }

    // ── GET /api/orders/{orderId} ─────────────────────────────────────────────

    /**
     * Get current order status.
     * Status progresses: PENDING → CONFIRMED or FAILED as saga completes.
     */
    @GetMapping("/{orderId}")
    public ResponseEntity<OrderDtos.OrderStatusResponse> getOrder(@PathVariable String orderId) {
        log.info("GET /api/orders/{}", orderId);
        Order order = orderService.getOrder(orderId);
        return ResponseEntity.ok(toStatusResponse(order));
    }

    // ── GET /api/orders/customer/{customerId} ─────────────────────────────────

    @GetMapping("/customer/{customerId}")
    public ResponseEntity<List<OrderDtos.OrderStatusResponse>> getCustomerOrders(
            @PathVariable String customerId) {
        log.info("GET /api/orders/customer/{}", customerId);
        List<OrderDtos.OrderStatusResponse> orders = orderService.getOrdersByCustomer(customerId)
                .stream().map(this::toStatusResponse).toList();
        return ResponseEntity.ok(orders);
    }

    // ── GET /api/orders ───────────────────────────────────────────────────────

    @GetMapping
    public ResponseEntity<List<OrderDtos.OrderStatusResponse>> getAllOrders() {
        List<OrderDtos.OrderStatusResponse> orders = orderService.getAllOrders()
                .stream().map(this::toStatusResponse).toList();
        return ResponseEntity.ok(orders);
    }

    // ── POST /api/orders/batch ────────────────────────────────────────────────

    /**
     * Place N orders with varied hardware products.
     * Useful for watching the saga, DLT, and compensation in action.
     *   curl -X POST "http://localhost:8082/api/orders/batch?count=10"
     */
    @PostMapping("/batch")
    public ResponseEntity<OrderDtos.BatchOrderResponse> placeBatch(
            @RequestParam(defaultValue = "5") int count,
            @RequestParam(defaultValue = "CUST-001") String customerId) {

        log.info("POST /api/orders/batch — count={} customerId={}", count, customerId);

        long start = System.currentTimeMillis();

        String[][] products = {
                {"PROD-DRILL-001",  "DeWalt 20V Cordless Drill",      "POWER_TOOLS",  "149.99"},
                {"PROD-SAW-002",    "Circular Saw 7-1/4 inch",        "POWER_TOOLS",  "89.99"},
                {"PROD-PIPE-010",   "3/4 inch Copper Pipe (10ft)",    "PLUMBING",     "24.99"},
                {"PROD-VALVE-011",  "Ball Valve 1/2 inch",            "PLUMBING",     "12.49"},
                {"PROD-WIRE-020",   "12 AWG Electrical Wire (100ft)", "ELECTRICAL",   "54.99"},
                {"PROD-PANEL-021",  "20A Circuit Breaker",            "ELECTRICAL",   "18.99"},
                {"PROD-NAIL-030",   "Framing Nails 3.5 inch (5lb)",   "FASTENERS",    "19.99"},
                {"PROD-SCREW-031",  "Deck Screws #10 x 3in (100pk)",  "FASTENERS",    "14.99"},
                {"PROD-LUMB-040",   "2x4x8 Pressure Treated Lumber",  "LUMBER",       "8.99"},
                {"PROD-PLY-041",    "3/4 inch Plywood Sheet",         "LUMBER",       "52.99"},
        };

        for (int i = 0; i < count; i++) {
            String[] p = products[i % products.length];
            OrderDtos.PlaceOrderRequest req = new OrderDtos.PlaceOrderRequest(
                    customerId,
                    customerId.toLowerCase().replace("-", ".") + "@example.com",
                    "+1-555-" + String.format("%04d", i + 1000),
                    List.of(new OrderDtos.OrderItemRequest(
                            p[0], p[1], p[2],
                            (i % 3) + 1,
                            new BigDecimal(p[3])
                    )),
                    (100 + i) + " Hardware Lane, Chicago IL 60601"
            );
            orderService.placeOrder(req);
        }

        long elapsed = System.currentTimeMillis() - start;
        log.info("Batch complete — {} orders placed in {}ms", count, elapsed);

        return ResponseEntity.accepted().body(new OrderDtos.BatchOrderResponse(
                count,
                elapsed,
                "Watch the saga at http://localhost:8080 → Consumer Groups"
        ));
    }

    // ── Exception Handlers ────────────────────────────────────────────────────

    @ExceptionHandler(OrderService.OrderNotFoundException.class)
    public ResponseEntity<Map<String, String>> handleNotFound(
            OrderService.OrderNotFoundException ex) {
        return ResponseEntity.status(HttpStatus.NOT_FOUND)
                .body(Map.of("error", ex.getMessage()));
    }

    @ExceptionHandler(Exception.class)
    public ResponseEntity<Map<String, String>> handleGeneral(Exception ex) {
        log.error("Unhandled exception", ex);
        return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR)
                .body(Map.of("error", "Internal server error: " + ex.getMessage()));
    }

    // ── Mapper ────────────────────────────────────────────────────────────────

    private OrderDtos.OrderStatusResponse toStatusResponse(Order order) {
        List<OrderDtos.OrderItemResponse> itemResponses = order.getItems().stream()
                .map(i -> new OrderDtos.OrderItemResponse(
                        i.getProductId(), i.getProductName(), i.getProductCategory(),
                        i.getQuantity(), i.getUnitPrice(), i.getSubtotal()
                ))
                .toList();

        return new OrderDtos.OrderStatusResponse(
                order.getId(),
                order.getCustomerId(),
                order.getStatus().name(),
                order.getTotalAmount(),
                order.getShippingAddress(),
                itemResponses,
                order.getFailureReason(),
                order.getPlacedAt() != null ? order.getPlacedAt().toString() : null,
                order.getConfirmedAt() != null ? order.getConfirmedAt().toString() : null
        );
    }
}
