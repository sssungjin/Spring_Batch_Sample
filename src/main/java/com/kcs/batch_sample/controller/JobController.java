package com.kcs.batch_sample.controller;

import com.kcs.batch_sample.dto.UserCreationDto;
import com.kcs.batch_sample.service.IndividualProcessingService;
import lombok.RequiredArgsConstructor;
import org.springframework.batch.core.*;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequiredArgsConstructor
@RequestMapping("/api/v1/job")
public class JobController {

    private final IndividualProcessingService individualProcessingService;

    @PostMapping("/individual-processing")
    public String runIndividualProcessingJob(@RequestBody UserCreationDto userCreationDto) throws Exception {
        JobExecution jobExecution = individualProcessingService.processIndividualJob(userCreationDto);
        return "Individual Processing Job completed with status: " + jobExecution.getStatus();
    }
}