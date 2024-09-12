package com.kcs.batch_sample.repository;

import com.kcs.batch_sample.domain.BatchLog;
import org.springframework.data.jpa.repository.JpaRepository;

public interface BatchLogRepository extends JpaRepository<BatchLog, Long> {
}