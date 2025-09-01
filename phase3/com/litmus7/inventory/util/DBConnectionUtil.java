package com.litmus7.inventory.util;


import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Properties;
import java.io.InputStream;
import java.io.IOException;

public class DBConnectionUtil {

    private static String URL;
    private static String USER;
    private static String PASSWORD;

    static {
    	

        try (InputStream input = DBConnectionUtil.class.getClassLoader().getResourceAsStream("config.properties")) {
            Properties prop = new Properties();
            if (input == null) {
                throw new RuntimeException("Unable to find config.properties");
            }
            prop.load(input);

            URL = prop.getProperty("db.url");
            USER = prop.getProperty("db.username");
            PASSWORD = prop.getProperty("db.password");

        } catch (IOException e) {
            throw new RuntimeException("Error reading database configuration", e);
        }
    }

    public static Connection getConnection() throws SQLException {
        return DriverManager.getConnection(URL, USER, PASSWORD);
    }
}
