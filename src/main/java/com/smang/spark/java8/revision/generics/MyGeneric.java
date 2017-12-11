package com.smang.spark.java8.revision.generics;

public class MyGeneric<T> {
    T input;

    public T getInput() {
        return input;
    }

    public void setInput(T input) {
        this.input = input;
    }

    public MyGeneric(T input) {
        this.input = input;
    }
}
