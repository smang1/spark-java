package com.smang.spark.java8.storage.fileFormats.pojos;

import org.apache.commons.lang.builder.ToStringBuilder;

import java.io.Serializable;

public class Contact implements Serializable {
    private String name;
    private Integer age;
    private String email;

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public Integer getAge() {
        return age;
    }

    public void setAge(Integer age) {
        this.age = age;
    }

    public String getEmail() {
        return email;
    }

    public void setEmail(String email) {
        this.email = email;
    }

    public Contact(String name, Integer age, String email) {
        this.name = name;
        this.age = age;
        this.email = email;
    }



    public String toString(String delimiter) {
        return String.format("%2$s%1$s%3$s%1$s%4$s", delimiter, this.name, this.age, this.email);
    }
}
