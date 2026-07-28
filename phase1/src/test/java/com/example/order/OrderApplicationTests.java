package com.example.order;

import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;

@SpringBootTest
class OrderApplicationTests {

    @Test
    void contextLoads() {
        // Verifies the Spring context wires up correctly:
        // entities, repositories, service, and controller all bean-scan
        // and the datasource connection is valid. Requires a running
        // Postgres instance matching application.properties.
    }
}
