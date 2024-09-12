package com.kcs.batch_sample.service;

import com.kcs.batch_sample.batch.job.GenericBatchProcessingJob;
import com.kcs.batch_sample.domain.User;
import com.kcs.batch_sample.repository.UserRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.core.*;
import org.springframework.batch.core.launch.JobLauncher;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.ItemWriter;
import org.springframework.batch.item.database.builder.JpaPagingItemReaderBuilder;
import org.springframework.stereotype.Service;

import jakarta.persistence.EntityManagerFactory;

@Service
@RequiredArgsConstructor
@Slf4j
public class UserEmailUpdateService {
    private final GenericBatchProcessingJob<User, User> genericBatchProcessingJob;
    private final JobLauncher jobLauncher;
    private final UserRepository userRepository;
    private final EntityManagerFactory entityManagerFactory;

    public JobExecution updateUserEmails() throws JobExecutionException {
        Job job = genericBatchProcessingJob.createJob(
                userReader(),
                userEmailProcessor(),
                userWriter(),
                "updateUserEmailJob",
                "updateUserEmailStep",
                5 // chunk size
        );

        JobParameters jobParameters = new JobParametersBuilder()
                .addLong("time", System.currentTimeMillis())
                .toJobParameters();

        return jobLauncher.run(job, jobParameters);
    }

    private ItemReader<User> userReader() {
        return new JpaPagingItemReaderBuilder<User>()
                .name("userReader")
                .entityManagerFactory(entityManagerFactory)
                .queryString("SELECT u FROM User u ORDER BY u.id DESC")
                .pageSize(15)
                .maxItemCount(15)
                .build();
    }

    private ItemProcessor<User, User> userEmailProcessor() {
        return user -> {
            String newEmail = "user" + user.getId() + "@example.com";
            user.setEmail(newEmail);
            log.info("Updated email for user {}: {}", user.getUsername(), newEmail);
            return user;
        };
    }

    private ItemWriter<User> userWriter() {
        return users -> {
            for (User user : users) {
                userRepository.save(user);
                log.info("User updated: {}", user);
            }
        };
    }
}