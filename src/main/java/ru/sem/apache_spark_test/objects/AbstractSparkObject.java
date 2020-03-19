package ru.sem.apache_spark_test.objects;

import org.apache.commons.csv.CSVPrinter;

import java.time.format.DateTimeFormatter;

public abstract class AbstractSparkObject {
    public static final DateTimeFormatter Date_formatter = DateTimeFormatter.ofPattern("yyyyMMdd");
    public static final DateTimeFormatter Date_time_formatter = DateTimeFormatter.ofPattern("yyyyMMdd"+"_HH");

    protected abstract void insertToCSV(CSVPrinter printer);

}
