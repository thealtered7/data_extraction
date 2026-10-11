package com.dataextraction.hello.impl;

import com.dataextraction.hello.Helloer;

public class FartHelloer implements Helloer {
    @Override
    public String sayHello(String name) {
        StringBuilder greeting = new StringBuilder();
        for (int i = 0; i < name.length(); i++) {
            greeting.append("thbbbbt").append(System.lineSeparator());
        }
        greeting.append("Thank you, ").append(name).append("!");
        return greeting.toString();
    }
}
