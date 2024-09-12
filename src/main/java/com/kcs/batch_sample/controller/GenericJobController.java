package com.kcs.batch_sample.controller;


import com.kcs.batch_sample.dto.UserBoardProcessingDto;
import com.kcs.batch_sample.dto.UserCreationDto;
import com.kcs.batch_sample.service.MultiEntityProcessingService;
import com.kcs.batch_sample.service.UserProcessingService;
import lombok.RequiredArgsConstructor;
import org.springframework.batch.core.JobExecution;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/api/v1/generic")
@RequiredArgsConstructor
public class GenericJobController {

    private final UserProcessingService userProcessingService;
    private final MultiEntityProcessingService multiEntityProcessingService;

    @PostMapping("/run-job1")
    public String runUserJob1(@RequestBody UserCreationDto userCreationDto) throws Exception {
        JobExecution jobExecution = userProcessingService.processUsers(userCreationDto, 10);
        return "Chunk processing completed with status: " + jobExecution.getStatus();
    }

    @PostMapping("/run-job3")
    public String runUserJob3(@RequestBody UserCreationDto userCreationDto) throws Exception {
        JobExecution jobExecution = userProcessingService.processUsers(userCreationDto, Integer.MAX_VALUE);
        return "Chunk processing completed with status: " + jobExecution.getStatus();
    }

    @PostMapping("/process-user-and-boards")
    public String processUserAndBoards(@RequestBody UserBoardProcessingDto userBoardProcessingDto) throws Exception {
        JobExecution jobExecution = multiEntityProcessingService.processUserAndBoards(userBoardProcessingDto, 1);
        return "Multi-entity processing completed with status: " + jobExecution.getStatus();
    }
}
