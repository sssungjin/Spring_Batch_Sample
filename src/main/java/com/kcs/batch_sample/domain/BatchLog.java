package com.kcs.batch_sample.domain;

import jakarta.persistence.*;
import lombok.*;

import java.time.LocalDateTime;

@Entity
@Table(name = "batch_log")
@Getter @Setter
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class BatchLog {
    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    @Column(name = "job_name")
    private String jobName;

    @Column(name = "step_name")
    private String stepName;

    @Column(name = "error_message")
    private String errorMessage;

    @Column(name = "item_data", columnDefinition = "TEXT")
    private String itemData;

    @Column(name = "created_at")
    private LocalDateTime createdAt;
}