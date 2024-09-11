package com.kcs.batch_sample.dto;

import java.util.List;

public record UserBoardProcessingDto(
        List<UserInfo> users,
        List<BoardInfo> boards
) {
    public record UserInfo(String username, String email) {}
    public record BoardInfo(String title, String content, String userUsername) {}
}