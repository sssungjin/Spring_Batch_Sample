package com.kcs.batch_sample.batch;

import com.kcs.batch_sample.domain.User;
import com.kcs.batch_sample.dto.UserCreationDto;
import com.kcs.batch_sample.dto.UserInfo;
import com.kcs.batch_sample.repository.UserRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.ItemWriter;
import org.springframework.batch.item.support.ListItemReader;
import org.springframework.stereotype.Component;

@Component
@RequiredArgsConstructor
@Slf4j
public class UserCreationChunk {

    private final UserRepository userRepository;

    public ItemReader<UserInfo> reader(UserCreationDto userCreationDto) {
        return new ListItemReader<>(userCreationDto.users());
    }

    public ItemProcessor<UserInfo, User> processor() {
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

    public ItemWriter<User> writer() {
        return users -> {
            for (User user : users) {
                User savedUser = userRepository.save(user);
                log.info("User saved: {}", savedUser);
            }
        };
    }
}