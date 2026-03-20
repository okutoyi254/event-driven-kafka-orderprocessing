package com.example.DistributedKafkaOrderProcessing.order;

import com.example.DistributedKafkaOrderProcessing.domain.entities.Order;
import com.example.DistributedKafkaOrderProcessing.domain.enums.OrderStatus;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

import java.util.List;

@Repository
public interface OrderRepository extends JpaRepository<Order,String> {

    List<Order> findByCustomerId(String customerId);

    List<Order> findByStatus(OrderStatus status);

    List<Order> findByCustomerIdOrderByPlacedAtDesc(String customerId);
}
