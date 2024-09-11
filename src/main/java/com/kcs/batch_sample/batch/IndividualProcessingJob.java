package com.kcs.batch_sample.batch;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.kcs.batch_sample.domain.User;
import com.kcs.batch_sample.dto.UserInfo;
import com.kcs.batch_sample.repository.UserRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.StepScope;
import org.springframework.batch.core.job.builder.JobBuilder;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.core.step.builder.StepBuilder;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.ItemWriter;
import org.springframework.batch.item.support.ListItemReader;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.transaction.PlatformTransactionManager;

import java.util.List;

@Configuration
@RequiredArgsConstructor
@Slf4j
public class IndividualProcessingJob {

    private final JobRepository jobRepository;
    private final PlatformTransactionManager transactionManager;
    private final UserRepository userRepository;
    private final ObjectMapper objectMapper;

    @Bean
    public Job processIndividualJob() throws Exception {
        return new JobBuilder("processIndividualJob", jobRepository)
                .start(processIndividualStep())
                .build();
    }

    @Bean
    public Step processIndividualStep() throws Exception {
        return new StepBuilder("processIndividualStep", jobRepository)
                .<UserInfo, User>chunk(1, transactionManager)
                .reader(userIndividualReader(null))
                .processor(userIndividualProcessor())
                .writer(userIndividualWriter())
                .faultTolerant()
                .skip(RuntimeException.class)
                .skipLimit(Integer.MAX_VALUE)
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
                throw new RuntimeException("Invalid email");
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
}