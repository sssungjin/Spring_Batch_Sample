package com.kcs.batch_sample.batch.job;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.kcs.batch_sample.domain.BatchLog;
import com.kcs.batch_sample.repository.BatchLogRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.core.*;
import org.springframework.batch.core.configuration.annotation.StepScope;
import org.springframework.batch.core.job.builder.JobBuilder;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.core.scope.context.ChunkContext;
import org.springframework.batch.core.step.builder.StepBuilder;
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
public class GenericMultiEntityBatchJob<T> {

    private final JobRepository jobRepository;
    private final PlatformTransactionManager transactionManager;
    private final ObjectMapper objectMapper;
    private final BatchLogRepository batchLogRepository;

    public Job processMultiEntityJob(
            String jobName,
            String stepName,
            ItemReader<T> reader,
            ItemProcessor<T, T> processor,
            ItemWriter<T> writer,
            int chunkSize) throws Exception {
        return new JobBuilder(jobName, jobRepository)
                .start(processMultiEntityStep(stepName, reader, processor, writer, chunkSize))
                .build();
    }

    private Step processMultiEntityStep(
            String stepName,
            ItemReader<T> reader,
            ItemProcessor<T, T> processor,
            ItemWriter<T> writer,
            int chunkSize) throws Exception {
        MultiEntityBatchLogListener<T> batchLogStepListener = new MultiEntityBatchLogListener<>(stepName);
        return new StepBuilder(stepName, jobRepository)
                .<T, T>chunk(chunkSize, transactionManager)
                .reader(reader)
                .processor(processor)
                .writer(writer)
                .faultTolerant()
                .skip(Exception.class)
                .skipLimit(10)
                .listener((StepExecutionListener) batchLogStepListener)
                .listener((ChunkListener) batchLogStepListener)
                .listener((SkipListener<T, T>) batchLogStepListener)
                .build();
    }

    @StepScope
    @Bean
    public ItemReader<T> multiEntityReader(@Value("#{jobParameters['dataJson']}") String dataJson, Class<T> dtoClass) throws Exception {
        T dto = objectMapper.readValue(dataJson, dtoClass);
        return new ListItemReader<>(List.of(dto));
    }

    private class MultiEntityBatchLogListener<T> implements StepExecutionListener, ChunkListener, SkipListener<T, T> {
        private final AtomicInteger totalData = new AtomicInteger(0);
        private final AtomicInteger successData = new AtomicInteger(0);
        private final AtomicInteger failureData = new AtomicInteger(0);
        private final String stepName;

        public MultiEntityBatchLogListener(String stepName) {
            this.stepName = stepName;
        }

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
            // Do nothing
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
        public void onSkipInProcess(T item, Throwable t) {
            failureData.incrementAndGet();
            saveBatchLog(null, "Item Skipped",
                    String.format("Item: %s, Error: %s", item.toString(), t.getMessage()));
        }

        private void saveBatchLog(JobExecution jobExecution, String message, String details) {
            try {
                BatchLog batchLog = BatchLog.builder()
                        .jobName(jobExecution != null ? jobExecution.getJobInstance().getJobName() : "Unknown Job")
                        .stepName(stepName)
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
}