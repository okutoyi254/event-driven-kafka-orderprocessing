package com.example.DistributedKafkaOrderProcessing.order;

import com.example.DistributedKafkaOrderProcessing.domain.Entities;
import com.example.DistributedKafkaOrderProcessing.domain.enums.OrderStatus;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

import java.util.List;

@Repository
public interface OrderRepository extends JpaRepository<Entities.Order,String> {

    List<Entities.Order> findByCustomerId(String customerId);

    List<Entities.Order> findByStatus(OrderStatus status);

    List<Entities.Order> findByCustomerIdOrderByPlacedAtDesc(String customerId);
}
