package com.smang.spark.java8.storage.fileFormats.pojos;

import java.util.stream.Stream;

public class Movie {
    private Integer id;
    private String title;
    private String genre;

    public Integer getId() {
        return id;
    }

    public void setId(Integer id) {
        this.id = id;
    }

    public String getTitle() {
        return title;
    }

    public void setTitle(String title) {
        this.title = title;
    }

    public String getGenre() {
        return genre;
    }

    public void setGenre(String genre) {
        this.genre = genre;
    }

    public Movie(Integer id, String title, String genre) {
        this.id = id;
        this.title = title;
        this.genre = genre;
    }

    public static Movie parseMovies(String line) {
        String[] fields = line.split(",");
        if (fields.length != 3) {
            System.out.println("The elements are :: ");
            Stream.of(fields).forEach(System.out::println);
            throw new IllegalArgumentException("Each line should contain only 3 fields where as current line has ::" + fields.length);
        }
        Integer movieId = Integer.parseInt(fields[0].trim());
        String movieTitle = fields[1].trim();
        String movieGenre = fields[2].trim();
        return new Movie(movieId,movieTitle,movieGenre);
    }
}
