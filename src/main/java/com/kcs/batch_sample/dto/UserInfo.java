package com.kcs.batch_sample.dto;

import com.fasterxml.jackson.annotation.JsonProperty;

public record UserInfo(

        @JsonProperty("username")
        String username,

        @JsonProperty("email")
        String email
) {
}
