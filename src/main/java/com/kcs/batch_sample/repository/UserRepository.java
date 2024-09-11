package com.kcs.batch_sample.repository;

import com.kcs.batch_sample.domain.User;
import org.springframework.data.jpa.repository.JpaRepository;

public interface UserRepository extends JpaRepository<User, Long> {
}