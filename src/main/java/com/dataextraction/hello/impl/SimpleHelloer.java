package com.dataextraction.hello.impl;

import com.dataextraction.hello.Helloer;

public class SimpleHelloer implements Helloer {
    @Override
    public String sayHello(String name) {
        return "Hello, " + name + "!";
    }
}
