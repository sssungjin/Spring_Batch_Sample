package com.kcs.batch_sample.controller;

import com.kcs.batch_sample.dto.UserCreationDto;
import com.kcs.batch_sample.service.IndividualProcessingService;
import com.kcs.batch_sample.service.UserEmailUpdateService;
import lombok.RequiredArgsConstructor;
import org.springframework.batch.core.*;
import org.springframework.web.bind.annotation.*;

@RestController
@RequiredArgsConstructor
@RequestMapping("/api/v1/job")
public class JobController {

    private final IndividualProcessingService individualProcessingService;
    private final UserEmailUpdateService userEmailUpdateService;

    @PostMapping("/individual-processing")
    public String runIndividualProcessingJob(@RequestBody UserCreationDto userCreationDto) throws Exception {
        JobExecution jobExecution = individualProcessingService.processIndividualJob(userCreationDto);
        return "Individual Processing Job completed with status: " + jobExecution.getStatus();
    }

    @GetMapping("/run-email-update-job")
    public String runUserEmailUpdateJob() throws Exception {
        JobExecution jobExecution = userEmailUpdateService.updateUserEmails();
        return "User email update job completed with status: " + jobExecution.getStatus();
    }
}