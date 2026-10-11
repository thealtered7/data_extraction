package com.dataextraction.hello;

import static org.junit.jupiter.api.Assertions.assertInstanceOf;

import com.dataextraction.hello.impl.FartHelloer;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;

@SpringBootTest(properties = {
        "spring.shell.interactive.enabled=false",
        "spring.shell.script.enabled=false",
        "dataextraction.helloer=fart"
})
class HelloConfigurationTest {

    @Autowired
    Helloer helloer;

    @Test
    void fartPropertySelectsFartHelloer() {
        assertInstanceOf(FartHelloer.class, helloer);
    }
}
