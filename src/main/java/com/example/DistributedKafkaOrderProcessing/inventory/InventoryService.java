package com.example.DistributedKafkaOrderProcessing.inventory;

import com.example.DistributedKafkaOrderProcessing.domain.Entities;
import com.example.DistributedKafkaOrderProcessing.domain.enums.InventoryStatus;
import com.example.DistributedKafkaOrderProcessing.domain.events.Events;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;


@Service
public class InventoryService {

    private static final Logger log= LoggerFactory.getLogger(InventoryService.class);

    private final InventoryItemRepository itemRepository;
    private final InventoryReservationRepository reservationRepository;

    public InventoryService(InventoryItemRepository itemRepository, InventoryReservationRepository reservationRepository) {
        this.itemRepository = itemRepository;
        this.reservationRepository = reservationRepository;
    }

    @Transactional
    public Events.InventoryResultEvent reserveInventory(Events.PaymentResultEvent paymentEvent,
                                                        List<Events.OrderItem> items){

        List<Events.ReservedItem> reserved=new ArrayList<>();
        List<String> outOfStock=new ArrayList<>();

//        Check availability for all items ordered
        for(Events.OrderItem item: items){
            Entities.InventoryItem stock= itemRepository
                    .findByIdForUpdate(item.productId())
                    .orElse(null);

            if(stock==null || !stock.canReserve(item.quantity())){
                outOfStock.add(item.productId());
                log.warn("Out of stock: productId={} requested={} available={}",
                        item.productId(),item.quantity(),stock !=null ? stock.getQuantityAvailable()- stock.getQuantityReserved() : 0);
            }
        }

//        Any item unavailable ->fail entire order
        if (!outOfStock.isEmpty()) {
            log.warn("Inventory insufficient for orderId={} outOfStock={}",
                    paymentEvent.orderId(), outOfStock);

            return new Events.InventoryResultEvent(
                    UUID.randomUUID().toString(),
                    paymentEvent.orderId(),
                    paymentEvent.customerId(),
                    paymentEvent.customerEmail(),
                    paymentEvent.transactionId(),
                    InventoryStatus.INSUFFICIENT_STOCK,
                    List.of(),
                    outOfStock,
                    "Items out of stock: " + String.join(", ", outOfStock),
                    Instant.now()
            );
        }

        // All available → reserve all
        for (Events.OrderItem item : items) {
            Entities.InventoryItem stock = itemRepository
                    .findByIdForUpdate(item.productId())
                    .orElseThrow();

            stock.reserve(item.quantity());
            itemRepository.save(stock);

            Entities.InventoryReservation reservation = new Entities.InventoryReservation(
                    paymentEvent.orderId(),
                    item.productId(),
                    item.productName(),
                    item.quantity(),
                    stock.getWarehouseLocation()
            );
            reservationRepository.save(reservation);

            reserved.add(new Events.ReservedItem(
                    item.productId(),
                    item.productName(),
                    item.quantity(),
                    stock.getWarehouseLocation()
            ));

            log.info("📦 Reserved: productId={} qty={} location={}",
                    item.productId(), item.quantity(), stock.getWarehouseLocation());
        }

        log.info("✅ Inventory reserved for orderId={} items={}",
                paymentEvent.orderId(), reserved.size());

        return new Events.InventoryResultEvent(
                UUID.randomUUID().toString(),
                paymentEvent.orderId(),
                paymentEvent.customerId(),
                paymentEvent.customerEmail(),
                paymentEvent.transactionId(),
                InventoryStatus.RESERVED,
                reserved,
                List.of(),
                null,
                Instant.now()
        );
    }

    @Transactional
    public void releaseInventory(String orderId, List<Events.ReservedItem> itemsToRelease) {
        if (itemsToRelease == null || itemsToRelease.isEmpty()) {
            log.info("No inventory to release for orderId={}", orderId);
            return;
        }

        for (Events.ReservedItem item : itemsToRelease) {
            // Release the stock
            itemRepository.findByIdForUpdate(item.productId()).ifPresent(stock -> {
                stock.release(item.quantityReserved());
                itemRepository.save(stock);
                log.info("Released: productId={} qty={}", item.productId(), item.quantityReserved());
            });

            // Mark reservation records as released
            reservationRepository
                    .findByOrderIdAndStatus(orderId, InventoryStatus.RESERVED)
                    .stream()
                    .filter(r -> r.getProductId().equals(item.productId()))
                    .forEach(r -> {
                        r.release();
                        reservationRepository.save(r);
                    });
        }

        log.info("Inventory released for orderId={} items={}",
                orderId, itemsToRelease.size());
    }
    }

