package org.example.kafka.api;

import lombok.Data;

@Data
public class User {
    private Long id;
    private String name;
    private Long timestamp;
}
