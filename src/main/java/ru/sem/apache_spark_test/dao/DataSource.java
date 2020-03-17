package ru.sem.apache_spark_test.dao;

import com.zaxxer.hikari.HikariDataSource;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.sql.Connection;

@Deprecated
public class DataSource {

    private static Logger logger = LogManager.getLogger(DataSource.class);
    private static final HikariDataSource dataSourcePsql;

    static {
        dataSourcePsql = new HikariDataSource();
        dataSourcePsql.setDriverClassName("org.postgresql.Driver");
        dataSourcePsql.setJdbcUrl(System.getProperty("psql.url", "jdbc:postgresql://192.168.56.101:5432/testdb"));
        dataSourcePsql.setUsername(System.getProperty("psql.user", "postgres"));
        dataSourcePsql.setPassword(System.getProperty("psql.pass", "123456"));
        dataSourcePsql.setMaximumPoolSize(Integer.parseInt(System.getProperty("psql.pool.size", "2")));
        dataSourcePsql.setMaxLifetime(30000);
    }

    public static HikariDataSource getDataSourcePsql() {
        return dataSourcePsql;
    }

    public static Connection getConnection() {
        try {
            return dataSourcePsql.getConnection();
        } catch (Exception z) {
            //FIXME: find better solution
            return null;
        }
    }
}
