package com.we.dreams.thousand;

import org.mybatis.spring.annotation.MapperScan;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication
@MapperScan("com.we.dreams.thousand.mapper*")
public class ThousandApplication {

    public static void main(String[] args) {
        SpringApplication.run(ThousandApplication.class, args);
    }

}
