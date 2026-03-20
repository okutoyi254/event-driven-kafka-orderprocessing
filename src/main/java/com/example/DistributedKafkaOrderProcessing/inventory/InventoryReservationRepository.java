package com.example.DistributedKafkaOrderProcessing.inventory;

import com.example.DistributedKafkaOrderProcessing.domain.entities.InventoryReservation;
import com.example.DistributedKafkaOrderProcessing.domain.enums.InventoryStatus;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

import java.util.List;
@Repository
public interface InventoryReservationRepository extends JpaRepository<InventoryReservation,Long> {

    List<InventoryReservation> findByOrderId(String orderId);

    List<InventoryReservation> findByOrderIdAndStatus(String orderId, InventoryStatus status);
}
