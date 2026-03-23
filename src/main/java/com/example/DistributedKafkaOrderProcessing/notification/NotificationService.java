package com.example.DistributedKafkaOrderProcessing.notification;

import com.example.DistributedKafkaOrderProcessing.domain.entities.Notification;
import com.example.DistributedKafkaOrderProcessing.domain.enums.NotificationType;
import com.example.DistributedKafkaOrderProcessing.domain.events.Events;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.List;

@Service
public class NotificationService {

    private static final Logger log = LoggerFactory.getLogger(NotificationService.class);

    private final NotificationRepository notificationRepository;

    @Value("${app.notification.sla-minutes:5}")
    private int slaMinutes;

    public NotificationService(NotificationRepository notificationRepository) {
        this.notificationRepository = notificationRepository;
    }

    // ── Notification builders — one per event type ────────────────────────────

    @Transactional
    public void notifyOrderReceived(Events.OrderPlacedEvent event) {
        if (alreadyNotified(event.orderId(), NotificationType.ORDER_RECEIVED)) return;

        Notification notification = new Notification(
                event.orderId(),
                event.customerId(),
                event.customerEmail(),
                event.customerPhone(),
                NotificationType.ORDER_RECEIVED,
                "Your hardware order has been received — #" + shortId(event.orderId()),
                buildOrderReceivedMessage(event),
                deadline(event.placedAt())
        );
        saveAndSend(notification);
    }

    @Transactional
    public void notifyPaymentSuccess(Events.PaymentResultEvent event) {
        if (alreadyNotified(event.orderId(), NotificationType.PAYMENT_SUCCESS)) return;

        Notification notification = new Notification(
                event.orderId(),
                event.customerId(),
                event.customerEmail(),
                null,
                NotificationType.PAYMENT_SUCCESS,
                "Payment confirmed for order #" + shortId(event.orderId()),
                String.format(
                        "Your payment of $%s has been confirmed (Transaction: %s). " +
                                "We are now checking inventory for your order.",
                        event.amountCharged(), event.transactionId()
                ),
                deadline(event.processedAt())
        );
        saveAndSend(notification);
    }

    @Transactional
    public void notifyPaymentFailed(Events.PaymentResultEvent event) {
        if (alreadyNotified(event.orderId(), NotificationType.PAYMENT_FAILED)) return;

        Notification notification = new Notification(
                event.orderId(),
                event.customerId(),
                event.customerEmail(),
                null,
                NotificationType.PAYMENT_FAILED,
                "Payment failed for order #" + shortId(event.orderId()),
                String.format(
                        "Unfortunately, your payment of $%s could not be processed. " +
                                "Reason: %s. Please try again with a different payment method.",
                        event.amountCharged(), event.failureReason()
                ),
                deadline(event.processedAt())
        );
        saveAndSend(notification);
    }

    @Transactional
    public void notifyInventoryReserved(Events.InventoryResultEvent event) {
        if (alreadyNotified(event.orderId(), NotificationType.INVENTORY_RESERVED)) return;

        int itemCount = event.reservedItems().size();
        Notification notification = new Notification(
                event.orderId(),
                event.customerId(),
                event.customerEmail(),
                null,
                NotificationType.INVENTORY_RESERVED,
                "Items reserved for order #" + shortId(event.orderId()),
                String.format(
                        "Great news! All %d item(s) in your order are in stock and reserved. " +
                                "We are now booking your delivery.",
                        itemCount
                ),
                deadline(event.processedAt())
        );
        saveAndSend(notification);
    }

    @Transactional
    public void notifyInventoryFailed(Events.InventoryResultEvent event) {
        if (alreadyNotified(event.orderId(), NotificationType.INVENTORY_FAILED)) return;

        Notification notification = new Notification(
                event.orderId(),
                event.customerId(),
                event.customerEmail(),
                null,
                NotificationType.INVENTORY_FAILED,
                "Items unavailable for order #" + shortId(event.orderId()),
                String.format(
                        "We're sorry, but some items in your order are currently out of stock: %s. " +
                                "Your payment has been refunded. Please check back soon or contact us.",
                        String.join(", ", event.outOfStockProductIds())
                ),
                deadline(event.processedAt())
        );
        saveAndSend(notification);
    }

    @Transactional
    public void notifyShippingBooked(Events.ShippingResultEvent event) {
        if (alreadyNotified(event.orderId(), NotificationType.SHIPPING_BOOKED)) return;

        Notification notification = new Notification(
                event.orderId(),
                event.customerId(),
                event.customerEmail(),
                null,
                NotificationType.SHIPPING_BOOKED,
                "Delivery scheduled for order #" + shortId(event.orderId()),
                String.format(
                        "Your delivery has been booked with %s. " +
                                "Tracking number: %s. " +
                                "Estimated delivery: %s.",
                        event.carrier(), event.trackingNumber(), event.estimatedDelivery()
                ),
                deadline(event.bookedAt())
        );
        saveAndSend(notification);
    }

    @Transactional
    public void notifyOrderConfirmed(Events.OrderConfirmedEvent event) {
        if (alreadyNotified(event.orderId(), NotificationType.ORDER_CONFIRMED)) return;

        Notification notification = new Notification(
                event.orderId(),
                event.customerId(),
                event.customerEmail(),
                event.customerPhone(),
                NotificationType.ORDER_CONFIRMED,
                "Order confirmed! #" + shortId(event.orderId()),
                buildOrderConfirmedMessage(event),
                deadline(event.confirmedAt())
        );
        saveAndSend(notification);
    }

    @Transactional
    public void notifyOrderFailed(Events.OrderFailedEvent event) {
        if (alreadyNotified(event.orderId(), NotificationType.ORDER_FAILED)) return;

        Notification notification = new Notification(
                event.orderId(),
                event.customerId(),
                event.customerEmail(),
                event.customerPhone(),
                NotificationType.ORDER_FAILED,
                "Order could not be completed — #" + shortId(event.orderId()),
                buildOrderFailedMessage(event),
                deadline(event.failedAt())
        );
        saveAndSend(notification);
    }

    // ── SLA Enforcer ──────────────────────────────────────────────────────────

    @Scheduled(fixedRateString = "${app.notification.scheduler-interval:30000}")
    @Transactional
    public void enforceSla() {
        Instant now = Instant.now();

        // ── Force overdue notifications ───────────────────────────────────────
        List<Notification> overdue = notificationRepository.findOverdueNotifications(now);
        if (!overdue.isEmpty()) {
            log.warn("⏰ SLA enforcer: {} overdue notification(s) found — forcing send", overdue.size());
            overdue.forEach(this::forceSend);
        }

        // ── Proactively send approaching-deadline notifications ───────────────
        Instant oneMinuteFromNow = now.plus(1, ChronoUnit.MINUTES);
        List<Notification> approaching = notificationRepository
                .findNotificationsApproachingDeadline(now, oneMinuteFromNow);

        if (!approaching.isEmpty()) {
            log.info("⏰ SLA enforcer: {} notification(s) approaching deadline — sending proactively",
                    approaching.size());
            approaching.forEach(this::forceSend);
        }
    }

    // ── Private helpers ───────────────────────────────────────────────────────

    /**
     * Attempts to send immediately and persists the result.
     * If send fails, saves as PENDING — SLA enforcer will retry.
     */
    private void saveAndSend(Notification notification) {
        boolean sent = attemptSend(notification);
        if (sent) {
            notification.markSent();
        }
        // If not sent → stays PENDING → SLA enforcer picks it up
        notificationRepository.save(notification);
    }


    private void forceSend(Notification notification) {
        boolean sent = attemptSend(notification);
        if (sent) {
            notification.markSent();
        } else {
            notification.markFailed();
        }
        notificationRepository.save(notification);

        if (!sent) {
            log.error("❌ Force send FAILED for notificationId={} orderId={} type={}",
                    notification.getId(), notification.getOrderId(), notification.getType());
        }
    }


    private boolean attemptSend(Notification notification) {
        try {
            // Simulate send latency
            Thread.sleep(20 + (long)(Math.random() * 30));

            // 5% failure simulation
            if (Math.random() < 0.05) {
                throw new RuntimeException("Simulated send failure");
            }

            log.info("📧 Notification sent: orderId={} type={} to={}",
                    notification,
                    notification.getType(),
                    notification.getCustomerEmail());
            return true;

        } catch (Exception e) {
            log.warn("⚠️  Notification send failed: orderId={} type={} retry={}",
                    notification.getOrderId(),
                    notification.getType(),
                    notification.getRetryCount() + 1);
            return false;
        }
    }

    private boolean alreadyNotified(String orderId, NotificationType type) {
        boolean exists = notificationRepository.existsByOrderIdAndType(orderId, type);
        if (exists) {
            log.debug("ℹ️  Notification already exists: orderId={} type={}", orderId, type);
        }
        return exists;
    }

    private Instant deadline(Instant from) {
        return from.plus(slaMinutes, ChronoUnit.MINUTES);
    }

    private String shortId(String orderId) {
        return orderId.substring(0, 8).toUpperCase();
    }

    // ── Message builders ──────────────────────────────────────────────────────

    private String buildOrderReceivedMessage(Events.OrderPlacedEvent event) {
        StringBuilder sb = new StringBuilder();
        sb.append(String.format(
                "Thank you for your order! We've received your hardware order " +
                        "totalling $%s and are processing your payment now.\n\n",
                event.totalAmount()
        ));
        sb.append("Items ordered:\n");
        event.items().forEach(item ->
                sb.append(String.format("  • %s (x%d) — $%s\n",
                        item.productName(), item.quantity(), item.subtotal()))
        );
        sb.append(String.format("\nShipping to: %s", event.shippingAddress()));
        return sb.toString();
    }

    private String buildOrderConfirmedMessage(Events.OrderConfirmedEvent event) {
        return String.format(
                "Your order is confirmed and on its way!\n\n" +
                        "Order #: %s\n" +
                        "Total charged: $%s\n" +
                        "Transaction: %s\n\n" +
                        "Delivery details:\n" +
                        "  Carrier:  %s\n" +
                        "  Tracking: %s\n" +
                        "  ETA:      %s\n\n" +
                        "Shipping to: %s\n\n" +
                        "Thank you for choosing our hardware store!",
                shortId(event.orderId()),
                event.totalAmount(),
                event.transactionId(),
                event.carrier(),
                event.trackingNumber(),
                event.estimatedDelivery(),
                event.shippingAddress()
        );
    }

    private String buildOrderFailedMessage(Events.OrderFailedEvent event) {
        StringBuilder sb = new StringBuilder();
        sb.append(String.format(
                "We're sorry, your order #%s could not be completed.\n\n" +
                        "Reason: %s\n\n",
                shortId(event.orderId()), event.failureReason()
        ));
        if (event.paymentRefunded()) {
            sb.append(String.format(
                    "A full refund of $%s has been processed and will appear " +
                            "in your account within 3–5 business days.\n\n",
                    event.totalAmount()
            ));
        }
        sb.append("Please contact our support team if you need assistance.");
        return sb.toString();
    }
}