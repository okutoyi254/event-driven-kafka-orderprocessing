package com.example.DistributedKafkaOrderProcessing.shipping;

import com.example.DistributedKafkaOrderProcessing.domain.entities.Shipment;
import com.example.DistributedKafkaOrderProcessing.domain.enums.ShippingStatus;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

import java.util.List;
import java.util.Optional;


@Repository
public interface ShippingRepository extends JpaRepository<Shipment,Long> {

    Optional<Shipment> findByOrderId(String orderId);

    List<Shipment> findByCustomerId(String customerId);

    List<Shipment>findByStatus(ShippingStatus status);
}
