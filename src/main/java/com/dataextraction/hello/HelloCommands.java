package com.dataextraction.hello;

import org.springframework.shell.standard.ShellComponent;
import org.springframework.shell.standard.ShellMethod;
import org.springframework.shell.standard.ShellOption;

@ShellComponent
public class HelloCommands {

    private final Helloer helloer;

    public HelloCommands(Helloer helloer) {
        this.helloer = helloer;
    }

    @ShellMethod(key = "hello", value = "Say hello")
    public String hello(@ShellOption(defaultValue = "world", help = "Name to greet") String name) {
        return helloer.sayHello(name);
    }
}
