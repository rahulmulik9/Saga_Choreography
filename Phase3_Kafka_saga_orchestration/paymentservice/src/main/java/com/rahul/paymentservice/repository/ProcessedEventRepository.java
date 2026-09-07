package com.rahul.paymentservice.repository;

import com.rahul.paymentservice.event.ProcessedEvent;
import org.springframework.data.jpa.repository.JpaRepository;

public interface ProcessedEventRepository extends JpaRepository<ProcessedEvent, Long> {
    boolean existsByEventKey(String eventKey);
}