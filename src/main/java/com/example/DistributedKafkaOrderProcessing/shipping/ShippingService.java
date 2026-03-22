package com.example.DistributedKafkaOrderProcessing.shipping;

import com.example.DistributedKafkaOrderProcessing.domain.entities.Shipment;
import com.example.DistributedKafkaOrderProcessing.domain.enums.ShippingStatus;
import com.example.DistributedKafkaOrderProcessing.domain.events.Events;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.math.BigDecimal;
import java.time.Instant;
import java.time.LocalDate;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.UUID;

@Service
public class ShippingService {

    private final static Logger log = LoggerFactory.getLogger(ShippingService.class);

    private final ShippingRepository shippingRepository;
    private final Random random = new Random();

    private static final List<String> CARRIERS = List.of(
            "FedEx Freight", "UPS Ground", "Home Depot Delivery", "XPO Logistics"
    );

    //    Delivery days by product category
    private static final Map<String, int[]> DELIVERY_DAYS = Map.of(
            "LUMBER", new int[]{3, 5},
            "POWER_TOOLS", new int[]{2, 4},
            "PLUMBING", new int[]{2, 3},
            "ELECTRICAL", new int[]{2, 3},
            "FASTENERS", new int[]{1, 2}
    );

    public ShippingService(ShippingRepository shippingRepository) {
        this.shippingRepository = shippingRepository;
    }

    @Transactional
    public Events.ShippingResultEvent bookShipment(Events.InventoryResultEvent inventoryEvent,
                                                   String shippingAddress,
                                                   BigDecimalWrapper totalAmount) {

//        5% - transient carrier API timeout -> NOT caught
        if (random.nextInt(20) == 0) {
            throw new CarrierApiTimeoutException("Carrier API timeout for order: " + inventoryEvent.orderId());
        }

//        15% - no carriers available in delivery area
        if (random.nextInt(100) < 15) {
            String reason = "No carrier slots available for address: " + shippingAddress;
            log.warn("Shipping UNAVAILABLE: orderId={}", inventoryEvent.orderId());

            Shipment shipment = new Shipment(
                    inventoryEvent.orderId(),
                    inventoryEvent.customerId(),
                    ShippingStatus.UNAVAILABLE,
                    null, null, null,
                    shippingAddress,
                    reason
            );

            shippingRepository.save(shipment);

            return new Events.ShippingResultEvent(
                    UUID.randomUUID().toString(),
                    inventoryEvent.orderId(),
                    inventoryEvent.customerId(),
                    inventoryEvent.customerEmail(),
                    inventoryEvent.transactionId(),
                    ShippingStatus.UNAVAILABLE,
                    null, null, null,
                    shippingAddress,
                    totalAmount.value(),
                    inventoryEvent.reservedItems(),
                    reason,
                    Instant.now()
            );
        }

//            80% booking successful
        String trackingNumber = generateTrackingNumber();
        String carrier = CARRIERS.get(random.nextInt(CARRIERS.size()));
        LocalDate estimatedDelivery = estimateDelivery(inventoryEvent.reservedItems());

        simulateWork(80, 200);

        Shipment shipment = new Shipment(
                inventoryEvent.orderId(),
                inventoryEvent.customerId(),
                ShippingStatus.BOOKED,
                trackingNumber,
                carrier,
                estimatedDelivery,
                shippingAddress,
                null
        );

        shippingRepository.save(shipment);

        log.info("Shipping BOOKED: orderId={} carrier={} tracking={} delivery={}",
                inventoryEvent.orderId(), carrier, trackingNumber, estimatedDelivery);

        return new Events.ShippingResultEvent(
                UUID.randomUUID().toString(),
                inventoryEvent.orderId(),
                inventoryEvent.customerId(),
                inventoryEvent.customerEmail(),
                inventoryEvent.transactionId(),
                ShippingStatus.BOOKED,
                trackingNumber,
                carrier,
                estimatedDelivery,
                shippingAddress,
                totalAmount.value(),
                inventoryEvent.reservedItems(),
                null,
                Instant.now()
        );
    }

    //    Cancel a booked shipment - called during saga compensation
    @Transactional
    public void cancelShipment(String orderId) {

        shippingRepository.findByOrderId(orderId).ifPresent(shipment -> {
            if (shipment.getStatus() == ShippingStatus.BOOKED) {
                shipment.cancel();
                shippingRepository.save(shipment);
                log.info("Shipment cancelled: orderId={} tracking={}",
                        orderId, shipment.getTrackingNumber());
            }
        });
    }

    private String generateTrackingNumber() {
        return "HW-" + UUID.randomUUID().toString().substring(0, 10).toUpperCase();
    }

    private LocalDate estimateDelivery(List<Events.ReservedItem> items) {
        int maxDays = 2;
        for (Events.ReservedItem item : items) {
            int[] range = estimateDaysForProduct(item.productId());
            if (range[1] > maxDays) maxDays = range[1];
        }

//        Week buffer
        LocalDate delivery = LocalDate.now().plusDays(maxDays);
        if (delivery.getDayOfWeek().getValue() == 6) delivery = delivery.plusDays(2); // Saturday
        if (delivery.getDayOfWeek().getValue() == 7) delivery = delivery.plusDays(1); // Sunday

        return delivery;
    }


    private int[] estimateDaysForProduct(String productId) {
        if (productId.contains("LUMB") || productId.contains("PLY"))
            return DELIVERY_DAYS.get("LUMBER");
        if (productId.contains("DRILL") || productId.contains("SAW"))
            return DELIVERY_DAYS.get("POWER_TOOLS");
        if (productId.contains("PIPE") || productId.contains("VALVE"))
            return DELIVERY_DAYS.get("PLUMBING");
        if (productId.contains("WIRE") || productId.contains("PANEL"))
            return DELIVERY_DAYS.get("ELECTRICAL");
        return DELIVERY_DAYS.getOrDefault("FASTENERS", new int[]{1, 2});
    }

    private void simulateWork(int minMs, int maxMs) {
        try {
            Thread.sleep(minMs + random.nextInt(maxMs - minMs));
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    public  record BigDecimalWrapper(BigDecimal value){}

    public static class CarrierApiTimeoutException extends RuntimeException{
        public CarrierApiTimeoutException(String message){
            super(message);
        }
    }

}
