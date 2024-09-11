package com.kcs.batch_sample.dto;

import java.util.List;

public record UserCreationDto(
        List<UserInfo> users
) {
}

