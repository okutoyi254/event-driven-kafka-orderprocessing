package com.example.DistributedKafkaOrderProcessing.payment;

import com.example.DistributedKafkaOrderProcessing.domain.Entities;
import com.example.DistributedKafkaOrderProcessing.domain.enums.PaymentStatus;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

import java.util.List;
import java.util.Optional;

@Repository
public interface PaymentRepository extends JpaRepository<Entities.Payment,Long> {

    Optional<Entities.Payment> findByOrderId(String orderId);
    List<Entities.Payment> findByCustomerId(String customerId);
    List<Entities.Payment> findByStatus(PaymentStatus status);
}
