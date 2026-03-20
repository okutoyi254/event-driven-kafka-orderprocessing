package com.example.DistributedKafkaOrderProcessing.inventory;

import com.example.DistributedKafkaOrderProcessing.domain.Entities;
import com.example.DistributedKafkaOrderProcessing.domain.enums.InventoryStatus;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

import java.util.List;
@Repository
public interface InventoryReservationRepository extends JpaRepository<Entities.InventoryReservation,Long> {

    List<Entities.InventoryReservation> findByOrderId(String orderId);

    List<Entities.InventoryReservation> findByOrderIdAndStatus(String orderId, InventoryStatus status);
}
