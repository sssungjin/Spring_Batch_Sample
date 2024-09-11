package com.kcs.batch_sample.controller;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.kcs.batch_sample.batch.UserCreationChunk;
import com.kcs.batch_sample.domain.User;
import com.kcs.batch_sample.dto.UserCreationDto;
import com.kcs.batch_sample.dto.UserInfo;
import lombok.RequiredArgsConstructor;
import org.springframework.batch.core.*;
import org.springframework.batch.core.job.builder.JobBuilder;
import org.springframework.batch.core.launch.JobLauncher;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.core.step.builder.StepBuilder;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;

@RestController
@RequiredArgsConstructor
@RequestMapping("/api/v1/job")
public class JobController {

    private final JobLauncher jobLauncher;
    private final Job createUsersJob;
    private final JobRepository jobRepository;
    private final PlatformTransactionManager transactionManager;
    private final UserCreationChunk userCreationChunk;
    private final ObjectMapper objectMapper;

    @PostMapping("/run-job")
    public String runJob(@RequestBody UserCreationDto userCreationDto) throws Exception {
        JobParameters jobParameters = new JobParametersBuilder()
                .addLong("time", System.currentTimeMillis())
                .addString("usersJson", objectMapper.writeValueAsString(userCreationDto.users()))
                .toJobParameters();
        JobExecution jobExecution = jobLauncher.run(createUsersJob, jobParameters);
        return "Job completed with status: " + jobExecution.getStatus();
    }

    @PostMapping("/run-chunk")
    public String runChunk(@RequestBody UserCreationDto userCreationDto) throws Exception {
        Step chunkStep = new StepBuilder("chunkStep", jobRepository)
                .<UserInfo, User>chunk(10, transactionManager)
                .reader(userCreationChunk.reader(userCreationDto))
                .processor(userCreationChunk.processor())
                .writer(userCreationChunk.writer())
                .build();

        JobParameters jobParameters = new JobParametersBuilder()
                .addLong("time", System.currentTimeMillis())
                .toJobParameters();

        Job job = new JobBuilder("chunkJob", jobRepository)
                .start(chunkStep)
                .build();

        JobExecution jobExecution = jobLauncher.run(job, jobParameters);
        return "Chunk processing completed with status: " + jobExecution.getStatus();
    }
}