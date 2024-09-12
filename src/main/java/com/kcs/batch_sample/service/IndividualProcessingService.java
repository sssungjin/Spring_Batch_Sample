package com.kcs.batch_sample.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.kcs.batch_sample.dto.UserCreationDto;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.JobParameters;
import org.springframework.batch.core.JobParametersBuilder;
import org.springframework.batch.core.launch.JobLauncher;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Service;

@Service
@Slf4j
public class IndividualProcessingService {

    private final JobLauncher jobLauncher;
    private final Job individualProcessingJob;
    private final ObjectMapper objectMapper;

    public IndividualProcessingService(
            JobLauncher jobLauncher,
            @Qualifier("processIndividualJob") Job individualProcessingJob,
            ObjectMapper objectMapper) {
        this.jobLauncher = jobLauncher;
        this.individualProcessingJob = individualProcessingJob;
        this.objectMapper = objectMapper;
    }

    public JobExecution processIndividualJob(UserCreationDto userCreationDto) throws Exception {
        JobParameters jobParameters = new JobParametersBuilder()
                .addLong("time", System.currentTimeMillis())
                .addString("usersJson", objectMapper.writeValueAsString(userCreationDto.users()))
                .toJobParameters();

        log.info("Starting individual processing job with parameters: {}", jobParameters);
        JobExecution jobExecution = jobLauncher.run(individualProcessingJob, jobParameters);
        log.info("Individual processing job completed with status: {}", jobExecution.getStatus());

        return jobExecution;
    }
}