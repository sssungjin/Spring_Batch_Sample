package com.kcs.batch_sample.service;

import com.kcs.batch_sample.batch.job.GenericBatchProcessingJob;
import com.kcs.batch_sample.domain.User;
import com.kcs.batch_sample.dto.UserCreationDto;
import com.kcs.batch_sample.dto.UserInfo;
import com.kcs.batch_sample.repository.UserRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.core.*;
import org.springframework.batch.core.launch.JobLauncher;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.ItemWriter;
import org.springframework.batch.item.support.ListItemReader;
import org.springframework.stereotype.Service;

@Service
@RequiredArgsConstructor
@Slf4j
public class UserProcessingService {
    private final GenericBatchProcessingJob<UserInfo, User> genericBatchProcessingJob;
    private final JobLauncher jobLauncher;
    private final UserRepository userRepository;

    public JobExecution processUsers(UserCreationDto userCreationDto, int chunkSize) throws JobExecutionException {
        Job job = genericBatchProcessingJob.createJob(
                userReader(userCreationDto),
                userProcessor(),
                userWriter(),
                "processUsersJob",
                "processUsersStep",
                chunkSize
        );

        JobParameters jobParameters = new JobParametersBuilder()
                .addLong("time", System.currentTimeMillis())
                .toJobParameters();

        return jobLauncher.run(job, jobParameters);
    }

    private ItemReader<UserInfo> userReader(UserCreationDto userCreationDto) {

        return new ListItemReader<>(userCreationDto.users());
    }

    private ItemProcessor<UserInfo, User> userProcessor() {
        return userInfo -> {
            if ("invalid_email".equals(userInfo.email())) {
                log.error("Invalid email for user: {}", userInfo.username());
                throw new RuntimeException("Invalid email for user: " + userInfo.username());
            }
            return User.builder().username(userInfo.username()).email(userInfo.email()).build();
        };
    }

    private ItemWriter<User> userWriter() {
        return users -> {
            for (User user : users) {
                User savedUser = userRepository.save(user);
                log.info("User saved: {}", savedUser);
            }
        };
    }
}
