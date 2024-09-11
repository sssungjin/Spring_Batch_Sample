package com.kcs.batch_sample.batch;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.kcs.batch_sample.domain.User;
import com.kcs.batch_sample.dto.UserInfo;
import com.kcs.batch_sample.repository.UserRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.core.*;
import org.springframework.batch.core.configuration.annotation.StepScope;
import org.springframework.batch.core.job.builder.JobBuilder;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.core.scope.context.StepSynchronizationManager;
import org.springframework.batch.core.step.builder.StepBuilder;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.ItemWriter;
import org.springframework.batch.item.support.ListItemReader;
import org.springframework.batch.repeat.RepeatStatus;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.transaction.PlatformTransactionManager;

import java.util.ArrayList;
import java.util.List;

@Configuration
@RequiredArgsConstructor
@Slf4j
public class UserCreationJob {

    private final JobRepository jobRepository;
    private final PlatformTransactionManager transactionManager;
    private final UserRepository userRepository;
    private final ObjectMapper objectMapper;

    @Bean
    public Job createUsersJob() throws Exception {
        return new JobBuilder("createUsersJob", jobRepository)
                .start(createUsersStep())
                .listener(new JobExecutionListener() {
                    @Override
                    public void beforeJob(JobExecution jobExecution) {
                        jobExecution.getExecutionContext().put("createdUserIds", new ArrayList<Long>());
                    }

                    @Override
                    public void afterJob(JobExecution jobExecution) {
                        if (jobExecution.getStatus() == BatchStatus.FAILED) {
                            log.error("Job failed. Please check the logs for details.");
                        } else {
                            log.info("Job completed successfully.");
                        }
                    }
                })
                .build();
    }

    @Bean
    public Step createUsersStep() throws Exception {
        return new StepBuilder("createUsersStep", jobRepository)
                .<UserInfo, User>chunk(10, transactionManager)
                .reader(userReader(null))
                .processor(userProcessor())
                .writer(userWriter())
                .build();
    }


    @Bean
    @StepScope
    public ItemReader<UserInfo> userReader(@Value("#{jobParameters['usersJson']}") String usersJson) throws Exception {
        List<UserInfo> users = objectMapper.readValue(usersJson, new TypeReference<List<UserInfo>>() {});
        return new ListItemReader<>(users);
    }

    @Bean
    public ItemProcessor<UserInfo, User> userProcessor() {
        return userInfo -> {
            User user = User.builder()
                    .username(userInfo.username())
                    .email(userInfo.email())
                    .build();
            log.info("Processing user: {}", user);
            if ("invalid_email".equals(userInfo.email())) {
                throw new RuntimeException("Simulated error with invalid email");
            }
            return user;
        };
    }

    @Bean
    public ItemWriter<User> userWriter() {
        return users -> {
            for (User user : users) {
                User savedUser = userRepository.save(user);
                log.info("User saved: {}", savedUser);
                JobExecution jobExecution = StepSynchronizationManager.getContext().getStepExecution().getJobExecution();
                List<Long> userIds = (List<Long>) jobExecution.getExecutionContext().get("createdUserIds");
                userIds.add(savedUser.getId());
            }
        };
    }
}