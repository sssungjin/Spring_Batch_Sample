package com.kcs.batch_sample.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.kcs.batch_sample.batch.job.GenericMultiEntityBatchJob;
import com.kcs.batch_sample.domain.Board;
import com.kcs.batch_sample.domain.User;
import com.kcs.batch_sample.dto.UserBoardProcessingDto;
import com.kcs.batch_sample.dto.UserInfo;
import com.kcs.batch_sample.repository.BoardRepository;
import com.kcs.batch_sample.repository.UserRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.core.*;
import org.springframework.batch.core.launch.JobLauncher;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.ItemWriter;
import org.springframework.stereotype.Service;

import java.util.HashMap;
import java.util.Map;

@Service
@RequiredArgsConstructor
@Slf4j
public class MultiEntityProcessingService {
    private final GenericMultiEntityBatchJob<UserBoardProcessingDto> genericMultiEntityBatchJob;
    private final JobLauncher jobLauncher;
    private final UserRepository userRepository;
    private final BoardRepository boardRepository;
    private final ObjectMapper objectMapper;

    public JobExecution processUserAndBoards(UserBoardProcessingDto userBoardProcessingDto, int chunkSize) throws Exception {
        Job job = genericMultiEntityBatchJob.processMultiEntityJob(
                "processUserAndBoardsJob",
                "processUserAndBoardsStep",
                multiEntityReader(userBoardProcessingDto),
                multiEntityProcessor(),
                multiEntityWriter(),
                chunkSize
        );

        JobParameters jobParameters = new JobParametersBuilder()
                .addLong("time", System.currentTimeMillis())
                .addString("dataJson", objectMapper.writeValueAsString(userBoardProcessingDto))
                .toJobParameters();

        return jobLauncher.run(job, jobParameters);
    }

    private ItemReader<UserBoardProcessingDto> multiEntityReader(UserBoardProcessingDto dto) {
        return new ItemReader<>() {
            private boolean processed = false;

            @Override
            public UserBoardProcessingDto read() {
                if (!processed) {
                    processed = true;
                    return dto;
                }
                return null;
            }
        };
    }

    private ItemProcessor<UserBoardProcessingDto, UserBoardProcessingDto> multiEntityProcessor() {
        return dto -> {
            log.info("Processing UserBoardProcessingDto: Users={}, Boards={}", dto.users().size(), dto.boards().size());
            for (UserInfo userInfo : dto.users()) {
                if ("invalid_email".equals(userInfo.email())) {
                    throw new RuntimeException("Invalid email for user: " + userInfo.username());
                }
            }
            return dto;
        };
    }

    private ItemWriter<UserBoardProcessingDto> multiEntityWriter() {
        return items -> {
            for (UserBoardProcessingDto dto : items) {
                Map<String, User> userMap = new HashMap<>();

                // Save users
                for (UserInfo userInfo : dto.users()) {
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