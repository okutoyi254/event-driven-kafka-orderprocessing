package com.example.DistributedKafkaOrderProcessing.order;

import jakarta.validation.Valid;
import jakarta.validation.constraints.*;

import java.math.BigDecimal;
import java.util.List;

public final class OrderDtos {

    private OrderDtos(){}

    public record PlaceOrderRequest(

            @NotBlank(message = "customerId is required")
            String customerId,

            @NotBlank(message = "customerEmail is required")
            @Email(message = "customerEmail must be a valid email address")
            String customerEmail,

            @NotBlank(message = "customerPhone is required")
            String customerPhone,

            @NotEmpty(message = "Order must contain at least one item")
            @Valid
            List<OrderItemRequest> items,

            @NotBlank(message = "shippingAddress is required")
            String shippingAddress
    ) {}

    public record OrderItemRequest(
            @NotBlank(message = "productId is required")
            String productId,

            @NotBlank(message = "productName is required")
            String productName,

            @NotBlank(message = "productCategory is required")
            String productCategory,

            @Min(value = 1, message = "quantity must be at least 1")
            int quantity,

            @NotNull(message = "unitPrice is required")
            @DecimalMin(value = "0.01", message = "unitPrice must be greater than 0")
            BigDecimal unitPrice
    ) {}

    public record PlaceOrderResponse(
            String orderId,
            String status,
            BigDecimal totalAmount,
            String message,
            String trackingUrl   // e.g. /api/orders/{orderId}
    ) {}

    public record OrderStatusResponse(
            String orderId,
            String customerId,
            String status,
            BigDecimal totalAmount,
            String shippingAddress,
            List<OrderItemResponse> items,
            String failureReason,
            String placedAt,
            String confirmedAt
    ) {}

    public record OrderItemResponse(
            String productId,
            String productName,
            String productCategory,
            int quantity,
            BigDecimal unitPrice,
            BigDecimal subtotal
    ) {}

    public record BatchOrderResponse(
            int placed,
            long elapsedMs,
            String tip
    ) {}
}
