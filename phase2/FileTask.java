package com.litmus7.inventory.service;

import java.io.*;
import java.nio.file.*;
import java.sql.*;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.litmus7.inventory.util.DBConnectionUtil;

public class FileTask implements Runnable {
    private static final Logger logger = LogManager.getLogger(FileTask.class);

    private final File file;
    private static final String PROCESSED_DIR = "processed";
    private static final String ERROR_DIR = "error";

    public FileTask(File file) {
        this.file = file;
    }

    @Override
    public void run() {
        logger.info("Processing file in thread {} → {}", Thread.currentThread().getName(), file.getName());

        boolean success = false;

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
                logger.info("File committed: {}", file.getName());

            } catch (Exception e) {
                try {
                    conn.rollback();
                } catch (SQLException sqle) {
                    logger.error("Rollback failed for {}: {}", file.getName(), sqle.getMessage());
                }
                logger.error("Error in file {} → rollback. Reason: {}", file.getName(), e.getMessage());
            }

        } catch (Exception ex) {
            logger.error("DB connection error for file {}: {}", file.getName(), ex.getMessage());
        }

        // Move file after processing
        if (success) {
            moveFile(PROCESSED_DIR);
        } else {
            moveFile(ERROR_DIR);
        }
    }

    private void moveFile(String targetDir) {
        try {
            Files.createDirectories(Paths.get(targetDir));
            Files.move(file.toPath(),
                       Paths.get(targetDir, file.getName()),
                       StandardCopyOption.REPLACE_EXISTING);
            logger.info("Moved file {} to {}", file.getName(), targetDir);
        } catch (IOException e) {
            logger.error("Failed to move file {}: {}", file.getName(), e.getMessage());
        }
    }
}
