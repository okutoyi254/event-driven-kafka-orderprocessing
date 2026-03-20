package com.example.DistributedKafkaOrderProcessing.inventory;

import com.example.DistributedKafkaOrderProcessing.domain.Entities;
import jakarta.persistence.LockModeType;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Lock;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;
import org.springframework.stereotype.Repository;

import java.util.Optional;

@Repository
public interface InventoryItemRepository extends JpaRepository<Entities.InventoryItem,String> {

//    PESSIMISTIC_WRITE lock
    @Lock(LockModeType.PESSIMISTIC_WRITE)
    @Query("SELECT i FROM InventoryItem i WHERE i.productId =:productId")
    Optional<Entities.InventoryItem> findByIdForUpdate(@Param("productId") String productId);
}
