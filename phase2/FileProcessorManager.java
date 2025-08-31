package com.litmus7.inventory.service;

import java.io.File;
import java.nio.file.*;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class FileProcessorManager extends Thread {
    private static final Logger logger = LogManager.getLogger(FileProcessorManager.class);

    private static final String INPUT_DIR = "input";

    @Override
    public void run() {
        try {
            Files.createDirectories(Paths.get(INPUT_DIR));

            File inputFolder = new File(INPUT_DIR);
            File[] files = inputFolder.listFiles((dir, name) -> name.endsWith(".csv"));

            if (files == null || files.length == 0) {
                logger.info("No files found in input folder.");
                return;
            }

            for (File file : files) {
                Thread worker = new Thread(new FileTask(file), "Worker-" + file.getName());
                worker.start();
                logger.info("Started thread {} for file {}", worker.getName(), file.getName());
            }

        } catch (Exception e) {
            logger.error("Error in FileProcessorManager: {}", e.getMessage());
        }
    }
}
