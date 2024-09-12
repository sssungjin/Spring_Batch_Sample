package com.kcs.batch_sample.dto;

import com.fasterxml.jackson.annotation.JsonProperty;

public record BoardInfo(

        @JsonProperty("title")
        String title,

        @JsonProperty("content")
        String content,

        @JsonProperty("userUsername")
        String userUsername
) {}
