package com.smang.spark.java8.revision.generics;

public class MyGenericDemo {
    public static void main(String[] args) {
        MyGeneric<String> obj1 = new MyGeneric<String>("hello");
        System.out.println(obj1.getInput());

        MyGeneric<Integer> obj2 = new MyGeneric<Integer>(1);
        System.out.println(obj2.getInput());
    }
}
