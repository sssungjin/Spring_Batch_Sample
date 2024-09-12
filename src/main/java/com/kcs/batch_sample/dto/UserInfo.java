package com.kcs.batch_sample.dto;

import com.fasterxml.jackson.annotation.JsonProperty;
import jakarta.validation.constraints.Email;

public record UserInfo(

        @JsonProperty("username")
        String username,

        @Email
        @JsonProperty("email")
        String email
) {
}
