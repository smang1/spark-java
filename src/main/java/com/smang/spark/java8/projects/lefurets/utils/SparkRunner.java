package com.smang.spark.java8.projects.lefurets.utils;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;

public class SparkRunner {
    public static final String PATH_TARIFS_CSV = "src/main/resources/lesfurets/tarifs.csv";

    public static final String PATH_PRICES_CSV = "src/main/resources/lesfurets/prices.csv";


    static {
        Logger.getRootLogger().setLevel(Level.WARN);
        Logger.getLogger("org").setLevel(Level.WARN);
        Logger.getLogger("akka").setLevel(Level.WARN);
    }
}
