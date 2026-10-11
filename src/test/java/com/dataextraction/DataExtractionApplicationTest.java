package com.dataextraction;

import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;

@SpringBootTest(properties = {
        "spring.shell.interactive.enabled=false",
        "spring.shell.script.enabled=false"
})
class DataExtractionApplicationTest {

    @Test
    void contextLoads() {
    }
}
