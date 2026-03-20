package com.example.DistributedKafkaOrderProcessing.payment;

import com.example.DistributedKafkaOrderProcessing.domain.entities.Payment;
import com.example.DistributedKafkaOrderProcessing.domain.enums.PaymentStatus;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

import java.util.List;
import java.util.Optional;

@Repository
public interface PaymentRepository extends JpaRepository<Payment,Long> {

    Optional<Payment> findByOrderId(String orderId);
    List<Payment> findByCustomerId(String customerId);
    List<Payment> findByStatus(PaymentStatus status);
}
