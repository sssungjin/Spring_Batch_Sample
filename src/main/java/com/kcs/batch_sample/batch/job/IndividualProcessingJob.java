package com.kcs.batch_sample.batch.job;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.kcs.batch_sample.domain.BatchLog;
import com.kcs.batch_sample.domain.User;
import com.kcs.batch_sample.dto.UserInfo;
import com.kcs.batch_sample.repository.BatchLogRepository;
import com.kcs.batch_sample.repository.UserRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.core.*;
import org.springframework.batch.core.configuration.annotation.StepScope;
import org.springframework.batch.core.job.builder.JobBuilder;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.core.scope.context.ChunkContext;
import org.springframework.batch.core.step.builder.StepBuilder;
import org.springframework.batch.item.Chunk;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.ItemWriter;
import org.springframework.batch.item.support.ListItemReader;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.transaction.PlatformTransactionManager;

import java.time.LocalDateTime;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

@Configuration
@RequiredArgsConstructor
@Slf4j
public class IndividualProcessingJob {

    private final JobRepository jobRepository;
    private final PlatformTransactionManager transactionManager;
    private final UserRepository userRepository;
    private final ObjectMapper objectMapper;
    private final BatchLogRepository batchLogRepository;

    @Bean(name = "processIndividualJob")
    public Job processIndividualJob() throws Exception {
        return new JobBuilder("processIndividualJob", jobRepository)
                .start(processIndividualStep())
                .listener(new BatchLogJobListener())
                .build();
    }

    @Bean
    public Step processIndividualStep() throws Exception {
        BatchLogStepListener batchLogStepListener = new BatchLogStepListener();
        return new StepBuilder("processIndividualStep", jobRepository)
                .<UserInfo, User>chunk(1, transactionManager)
                .reader(userIndividualReader(null))
                .processor(userIndividualProcessor())
                .writer(userIndividualWriter())
                .faultTolerant()
                .skip(RuntimeException.class)
                .skipLimit(Integer.MAX_VALUE)
                .listener((StepExecutionListener) batchLogStepListener)
                .listener((ChunkListener) batchLogStepListener)
                .listener((SkipListener<UserInfo, User>) batchLogStepListener)
                .build();
    }

    @Bean
    @StepScope
    public ItemReader<UserInfo> userIndividualReader(@Value("#{jobParameters['usersJson']}") String usersJson) throws Exception {
        List<UserInfo> users = objectMapper.readValue(usersJson, new TypeReference<List<UserInfo>>() {});
        return new ListItemReader<>(users);
    }

    @Bean
    public ItemProcessor<UserInfo, User> userIndividualProcessor() {
        return userInfo -> {
            log.info("Processing user: {}", userInfo);
            if ("invalid_email".equals(userInfo.email())) {
                throw new RuntimeException("Invalid email for user: " + userInfo.username());
            }
            return User.builder().username(userInfo.username()).email(userInfo.email()).build();
        };
    }

    @Bean
    public ItemWriter<User> userIndividualWriter() {
        return users -> {
            for (User user : users) {
                User savedUser = userRepository.save(user);
                log.info("User saved: {}", savedUser);
            }
        };
    }

    private class BatchLogJobListener implements JobExecutionListener {
        @Override
        public void beforeJob(JobExecution jobExecution) {
           //saveBatchLog(jobExecution, "Job Started", null);
        }

        @Override
        public void afterJob(JobExecution jobExecution) {
           //saveBatchLog(jobExecution, "Job Finished", null);
        }
    }

    private class BatchLogStepListener implements StepExecutionListener, ChunkListener, SkipListener<UserInfo, User> {
        private final AtomicInteger totalData = new AtomicInteger(0);
        private final AtomicInteger successData = new AtomicInteger(0);
        private final AtomicInteger failureData = new AtomicInteger(0);

        @Override
        public void beforeStep(StepExecution stepExecution) {
            totalData.set(0);
            successData.set(0);
            failureData.set(0);
           // saveBatchLog(stepExecution.getJobExecution(), "Step Started", null);
        }

        @Override
        public ExitStatus afterStep(StepExecution stepExecution) {
            saveBatchLog(stepExecution.getJobExecution(), "Step Finished",
                    String.format("Total: %d, Success: %d, Failure: %d",
                            totalData.get(), successData.get(), failureData.get()));
            return ExitStatus.COMPLETED;
        }

        @Override
        public void beforeChunk(ChunkContext context) {
            //totalData.incrementAndGet();
        }

        @Override
        public void afterChunk(ChunkContext context) {
            int itemCount = (int) context.getStepContext().getStepExecution().getReadCount();
            totalData.set(itemCount);
            successData.set(itemCount - failureData.get());
        }

        @Override
        public void afterChunkError(ChunkContext context) {
            Throwable exception = (Throwable) context.getAttribute("exception");
            saveBatchLog(context.getStepContext().getStepExecution().getJobExecution(),
                    "Chunk Error", exception != null ? exception.getMessage() : "Unknown error");
        }

        @Override
        public void onSkipInProcess(UserInfo item, Throwable t) {
            failureData.incrementAndGet();
            saveBatchLog(null, "Item Skipped",
                    String.format("User: %s, Error: %s", item.username(), t.getMessage()));
        }
    }

    private void saveBatchLog(JobExecution jobExecution, String message, String details) {
        try {
            BatchLog batchLog = BatchLog.builder()
                    .jobName(jobExecution != null ? jobExecution.getJobInstance().getJobName() : "Unknown Job")
                    .stepName("processIndividualStep")
                    .errorMessage(message) // status 대신 errorMessage 사용
                    .itemData(details)
                    .createdAt(LocalDateTime.now())
                    .build();
            batchLogRepository.save(batchLog);
        } catch (Exception e) {
            log.error("Error saving batch log", e);
        }
    }
}