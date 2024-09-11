package com.kcs.batch_sample.batch;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.kcs.batch_sample.domain.Board;
import com.kcs.batch_sample.domain.User;
import com.kcs.batch_sample.dto.UserBoardProcessingDto;
import com.kcs.batch_sample.repository.BoardRepository;
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

import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Configuration
@RequiredArgsConstructor
@Slf4j
public class UserBoardProcessingJob {

    private final JobRepository jobRepository;
    private final PlatformTransactionManager transactionManager;
    private final UserRepository userRepository;
    private final BoardRepository boardRepository;
    private final ObjectMapper objectMapper;

    @Bean
    public Job processUserBoardJob() throws Exception {
        return new JobBuilder("processUserBoardJob", jobRepository)
                .start(processUserBoardStep())
                .build();
    }

    @Bean
    public Step processUserBoardStep() throws Exception {
        return new StepBuilder("processUserBoardStep", jobRepository)
                .<UserBoardProcessingDto, UserBoardProcessingDto>chunk(1, transactionManager)
                .reader(userBoardReader(null))
                .processor(userBoardProcessor())
                .writer(userBoardWriter())
                .build();
    }

    @Bean
    @StepScope
    public ItemReader<UserBoardProcessingDto> userBoardReader(
            @Value("#{jobParameters['userBoardDataJson']}") String userBoardDataJson) throws Exception {
        UserBoardProcessingDto dto = objectMapper.readValue(userBoardDataJson, UserBoardProcessingDto.class);
        return new ListItemReader<>(List.of(dto));
    }

    @Bean
    public ItemProcessor<UserBoardProcessingDto, UserBoardProcessingDto> userBoardProcessor() {
        return dto -> {
            log.info("Processing users: {}", dto.users().size());
            log.info("Processing boards: {}", dto.boards().size());

            for (UserBoardProcessingDto.UserInfo userInfo : dto.users()) {
                if ("invalid_email".equals(userInfo.email())) {
                    throw new RuntimeException("Invalid email for user: " + userInfo.username());
                }
            }
            return dto;
        };
    }

    @Bean
    public ItemWriter<UserBoardProcessingDto> userBoardWriter() {
        return items -> {
            for (UserBoardProcessingDto dto : items) {
                Map<String, User> userMap = new HashMap<>();

                // Save users
                for (UserBoardProcessingDto.UserInfo userInfo : dto.users()) {
                    User user = User.builder()
                            .username(userInfo.username())
                            .email(userInfo.email())
                            .build();
                    User savedUser = userRepository.save(user);
                    userMap.put(savedUser.getUsername(), savedUser);
                    log.info("User saved: {}", savedUser);
                }

                // Save boards
                for (UserBoardProcessingDto.BoardInfo boardInfo : dto.boards()) {
                    User user = userMap.get(boardInfo.userUsername());
                    if (user == null) {
                        throw new RuntimeException("User not found for board: " + boardInfo.title());
                    }
                    Board board = Board.builder()
                            .title(boardInfo.title())
                            .content(boardInfo.content())
                            .user(user)
                            .build();
                    Board savedBoard = boardRepository.save(board);
                    log.info("Board saved: {}", savedBoard);

                    if ("error".equals(boardInfo.title())) {
                        throw new RuntimeException("Error in board processing");
                    }
                }
            }
        };
    }
}