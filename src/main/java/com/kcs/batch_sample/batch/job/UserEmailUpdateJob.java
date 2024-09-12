package com.kcs.batch_sample.batch.job;

import com.kcs.batch_sample.domain.BatchLog;
import com.kcs.batch_sample.domain.User;
import com.kcs.batch_sample.repository.BatchLogRepository;
import com.kcs.batch_sample.repository.UserRepository;
import jakarta.persistence.EntityManagerFactory;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.core.*;
import org.springframework.batch.core.job.builder.JobBuilder;
import org.springframework.batch.core.launch.JobLauncher;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.core.scope.context.ChunkContext;
import org.springframework.batch.core.step.builder.StepBuilder;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.ItemWriter;
import org.springframework.batch.item.database.JpaPagingItemReader;
import org.springframework.batch.item.database.builder.JpaPagingItemReaderBuilder;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.transaction.PlatformTransactionManager;

import java.time.LocalDateTime;
import java.util.concurrent.atomic.AtomicInteger;

@Configuration
@RequiredArgsConstructor
@Slf4j
public class UserEmailUpdateJob {

    private final JobRepository jobRepository;
    private final PlatformTransactionManager transactionManager;
    private final UserRepository userRepository;
    private final BatchLogRepository batchLogRepository;
    private final EntityManagerFactory entityManagerFactory;
    private final JobLauncher jobLauncher;

    @Bean(name = "updateUserEmailJob")
    public Job updateUserEmailJob() {
        return new JobBuilder("updateUserEmailJob", jobRepository)
                .start(updateUserEmailStep())
                .listener(new BatchLogJobListener())
                .build();
    }

    @Bean
    public Step updateUserEmailStep() {
        BatchLogStepListener batchLogStepListener = new BatchLogStepListener();
        return new StepBuilder("updateUserEmailStep", jobRepository)
                .<User, User>chunk(5, transactionManager)
                .reader(userReader())
                .processor(userEmailProcessor())
                .writer(userWriter())
                .listener((StepExecutionListener) batchLogStepListener)
                .listener((ChunkListener) batchLogStepListener)
                .listener((SkipListener<User, User>) batchLogStepListener)
                .build();
    }

    @Bean
    public JpaPagingItemReader<User> userReader() {
        return new JpaPagingItemReaderBuilder<User>()
                .name("userReader")
                .entityManagerFactory(entityManagerFactory)
                .queryString("SELECT u FROM User u ORDER BY u.id DESC")
                .pageSize(15)
                .maxItemCount(15)
                .build();
    }

    @Bean
    public ItemProcessor<User, User> userEmailProcessor() {
        return user -> {
            String newEmail = "user" + user.getId() + "@example.com";
            user.setEmail(newEmail);
            log.info("Updated email for user {}: {}", user.getUsername(), newEmail);
            return user;
        };
    }

    @Bean
    public ItemWriter<User> userWriter() {
        return users -> {
            for (User user : users) {
                userRepository.save(user);
                log.info("User updated: {}", user);
            }
        };
    }

    //@Scheduled(cron = "0 0 * * * ?") // Run every hour
    // every minute
    //@Scheduled(cron = "0 * * * * ?") // Run every minute
    public void runJob() throws Exception {
        JobParameters jobParameters = new JobParametersBuilder()
                .addLong("time", System.currentTimeMillis())
                .toJobParameters();
        jobLauncher.run(updateUserEmailJob(), jobParameters);
    }

    private class BatchLogJobListener implements JobExecutionListener {
        @Override
        public void beforeJob(JobExecution jobExecution) {
            // Job 시작 로그
        }

        @Override
        public void afterJob(JobExecution jobExecution) {
            // Job 종료 로그
        }
    }

    private class BatchLogStepListener implements StepExecutionListener, ChunkListener, SkipListener<User, User> {
        private final AtomicInteger totalData = new AtomicInteger(0);
        private final AtomicInteger successData = new AtomicInteger(0);
        private final AtomicInteger failureData = new AtomicInteger(0);

        @Override
        public void beforeStep(StepExecution stepExecution) {
            totalData.set(0);
            successData.set(0);
            failureData.set(0);
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
            // Chunk 시작 전 처리
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
        public void onSkipInProcess(User item, Throwable t) {
            failureData.incrementAndGet();
            saveBatchLog(null, "Item Skipped",
                    String.format("User: %s, Error: %s", item.getUsername(), t.getMessage()));
        }
    }

    private void saveBatchLog(JobExecution jobExecution, String message, String details) {
        try {
            BatchLog batchLog = BatchLog.builder()
                    .jobName(jobExecution != null ? jobExecution.getJobInstance().getJobName() : "Unknown Job")
                    .stepName("userEmailUpdateStep")
                    .errorMessage(message)
                    .itemData(details)
                    .createdAt(LocalDateTime.now())
                    .build();
            batchLogRepository.save(batchLog);
        } catch (Exception e) {
            log.error("Error saving batch log", e);
        }
    }
}