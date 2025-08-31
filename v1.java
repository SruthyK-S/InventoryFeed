package com.litmus7.inventory.service;

import java.io.*;
import java.nio.file.*;
import java.sql.*;
import java.util.logging.*;



import com.litmus7.inventory.util.DBConnectionUtil;

public class FileProcessorThread extends Thread {
	private static final Logger logger = Logger.getLogger(FileProcessorThread.class.getName());
    
    private static final String INPUT_DIR = "input";
    private static final String PROCESSED_DIR = "processed";
    private static final String ERROR_DIR = "error";

    @Override
    public void run() {
        try {
            Files.createDirectories(Paths.get(PROCESSED_DIR));
            Files.createDirectories(Paths.get(ERROR_DIR));

         
            File inputFolder = new File(INPUT_DIR);
            File[] files = inputFolder.listFiles((dir, name) -> name.endsWith(".csv"));

            if (files == null || files.length == 0) {
                logger.info("No files found in input folder.");
                return;
            }

            for (File file : files) {
                processFile(file);
            }

        } catch (Exception e) {
            logger.severe("Error in thread: " + e.getMessage());
        }
    }

    private void processFile(File file) {
        logger.info("Processing file: " + file.getName());

        boolean success = false; // track success

        try (Connection conn = DBConnectionUtil.getConnection()) {
            conn.setAutoCommit(false);

            try (
                BufferedReader br = new BufferedReader(new FileReader(file));
                PreparedStatement ps = conn.prepareStatement(
                    "INSERT INTO products (sku, product_name, quantity, price) VALUES (?, ?, ?, ?)")
            ) {
              
                String line = br.readLine(); 

                while ((line = br.readLine()) != null) {
                    String[] parts = line.split(",");
                    if (parts.length < 4) {
                        throw new SQLException("Invalid record format: " + line);
                    }

                    ps.setInt(1, Integer.parseInt(parts[0].trim()));
                    ps.setString(2, parts[1].trim());
                    ps.setInt(3, Integer.parseInt(parts[2].trim()));
                    ps.setDouble(4, Double.parseDouble(parts[3].trim()));
                    ps.executeUpdate();
                }

                conn.commit();
                success = true;
                logger.info("File committed: " + file.getName());

            } catch (Exception e) {
                try {
                    conn.rollback();
                } catch (SQLException sqle) {
                    logger.severe("Rollback failed for " + file.getName() + ": " + sqle.getMessage());
                }
                logger.warning("Error in file " + file.getName() + " → rollback. Reason: " + e.getMessage());
            }

        } catch (Exception ex) {
            logger.severe("DB connection error for file " + file.getName() + ": " + ex.getMessage());
        }

     
        if (success) {
            moveFile(file, PROCESSED_DIR);
        } else {
            moveFile(file, ERROR_DIR);
        }
    }


    private void moveFile(File file, String targetDir) {
        try {
            Files.move(file.toPath(),
                    Paths.get(targetDir, file.getName()),
                    StandardCopyOption.REPLACE_EXISTING);
        } catch (IOException e) {
            logger.severe("Failed to move file " + file.getName() + ": " + e.getMessage());
        }
    }
}
