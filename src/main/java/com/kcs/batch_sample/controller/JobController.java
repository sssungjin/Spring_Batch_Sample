package com.kcs.batch_sample.controller;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.kcs.batch_sample.batch.UserCreationChunk;
import com.kcs.batch_sample.domain.User;
import com.kcs.batch_sample.dto.UserBoardProcessingDto;
import com.kcs.batch_sample.dto.UserCreationDto;
import com.kcs.batch_sample.dto.UserInfo;
import lombok.RequiredArgsConstructor;
import org.springframework.batch.core.*;
import org.springframework.batch.core.job.builder.JobBuilder;
import org.springframework.batch.core.launch.JobLauncher;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.core.step.builder.StepBuilder;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
//@RequiredArgsConstructor
@RequestMapping("/api/v1/job")
public class JobController {

    private final JobLauncher jobLauncher;
    private final Job createUsersJob;
    private final Job userBoardProcessingJob;
    private final Job individualProcessingJob;
    private final Job entireProcessingJob;

    private final JobRepository jobRepository;
    private final PlatformTransactionManager transactionManager;
    private final UserCreationChunk userCreationChunk;
    private final ObjectMapper objectMapper;

    public JobController(JobLauncher jobLauncher,
                         @Qualifier("createUsersJob") Job createUsersJob,
                         @Qualifier("processUserBoardJob") Job userBoardProcessingJob,
                         @Qualifier("processIndividualJob") Job individualProcessingJob,
                         @Qualifier("processEntireJob") Job entireProcessingJob,
                         JobRepository jobRepository,
                         PlatformTransactionManager transactionManager,
                         UserCreationChunk userCreationChunk,
                         ObjectMapper objectMapper) {
        this.jobLauncher = jobLauncher;
        this.createUsersJob = createUsersJob;
        this.userBoardProcessingJob = userBoardProcessingJob;
        this.individualProcessingJob = individualProcessingJob;
        this.entireProcessingJob = entireProcessingJob;
        this.jobRepository = jobRepository;
        this.transactionManager = transactionManager;
        this.userCreationChunk = userCreationChunk;
        this.objectMapper = objectMapper;
    }


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

    @PostMapping("/user-board-processing")
    public String runUserBoardProcessingJob(@RequestBody UserBoardProcessingDto userBoardProcessingDto) throws Exception {
        JobParameters jobParameters = new JobParametersBuilder()
                .addLong("time", System.currentTimeMillis())
                .addString("userBoardDataJson", objectMapper.writeValueAsString(userBoardProcessingDto))
                .toJobParameters();
        JobExecution jobExecution = jobLauncher.run(userBoardProcessingJob, jobParameters);
        return "User and Board Processing Job completed with status: " + jobExecution.getStatus();
    }

    @PostMapping("/individual-processing")
    public String runIndividualProcessingJob(@RequestBody UserCreationDto userCreationDto) throws Exception {
        JobParameters jobParameters = new JobParametersBuilder()
                .addLong("time", System.currentTimeMillis())
                .addString("usersJson", objectMapper.writeValueAsString(userCreationDto.users()))
                .toJobParameters();
        JobExecution jobExecution = jobLauncher.run(individualProcessingJob, jobParameters);
        return "Individual Processing Job completed with status: " + jobExecution.getStatus();
    }

    @PostMapping("/entire-processing")
    public String runEntireProcessingJob(@RequestBody UserCreationDto userCreationDto) throws Exception {
        JobParameters jobParameters = new JobParametersBuilder()
                .addLong("time", System.currentTimeMillis())
                .addString("usersJson", objectMapper.writeValueAsString(userCreationDto.users()))
                .toJobParameters();
        JobExecution jobExecution = jobLauncher.run(entireProcessingJob, jobParameters);
        return "All or Nothing Processing Job completed with status: " + jobExecution.getStatus();
    }
}