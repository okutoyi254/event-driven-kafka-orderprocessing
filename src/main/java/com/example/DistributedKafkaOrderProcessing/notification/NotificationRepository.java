package com.example.DistributedKafkaOrderProcessing.notification;

import com.example.DistributedKafkaOrderProcessing.domain.entities.Notification;
import com.example.DistributedKafkaOrderProcessing.domain.enums.NotificationType;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;
import org.springframework.stereotype.Repository;

import java.time.Instant;
import java.util.List;

@Repository
public interface NotificationRepository extends JpaRepository<Notification,Long> {

    List<Notification> findByOrderId(String orderId);

    List<Notification> findByCustomerId(String customerId);

    @Query("""
            SELECT n FROM Notification n
            WHERE n.status IN ('PENDING', 'FAILED')
            AND n.deadline < :now
            ORDER BY n.deadline ASC
            """)
    List<Notification> findOverdueNotifications(@Param("now") Instant now);

    /**
     * Finds PENDING notifications approaching their deadline (within 1 minute).
     * Allows proactive sending before the SLA is breached.
     */
    @Query("""
            SELECT n FROM Notification n
            WHERE n.status = 'PENDING'
            AND n.deadline BETWEEN :now AND :soonCutoff
            ORDER BY n.deadline ASC
            """)
    List<Notification> findNotificationsApproachingDeadline(
            @Param("now") Instant now,
            @Param("soonCutoff") Instant soonCutoff
    );

    boolean existsByOrderIdAndType(String orderId, NotificationType type);
}