package com.litmus7.inventory.app;

import com.litmus7.inventory.service.FileProcessorManager;
public class Main {
    public static void main(String[] args) {
        FileProcessorThread processor = new FileProcessorThread();
        processor.start();
    }
}
