package com.litmus7.inventory.service;

import java.io.File;
import java.nio.file.*;
import java.util.concurrent.*;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class FileProcessorManager {
    private static final Logger logger = LogManager.getLogger(FileProcessorManager.class);
    private static final String INPUT_DIR = "input";

    public void startProcessing() {
        try {
            Files.createDirectories(Paths.get(INPUT_DIR));

            File inputFolder = new File(INPUT_DIR);
            File[] files = inputFolder.listFiles((dir, name) -> name.endsWith(".csv"));

            if (files == null || files.length == 0) {
                logger.info("No files found in input folder.");
                return;
            }

            ExecutorService executor = Executors.newFixedThreadPool(5);

            for (File file : files) {
                executor.submit(new FileTask(file));
            }

            executor.shutdown();
            if (!executor.awaitTermination(1, TimeUnit.HOURS)) {
                logger.warn("Timeout reached before all files were processed.");
            }

        } catch (Exception e) {
            logger.error("Error in FileProcessorManager: " + e.getMessage(), e);
        }
    }
}
