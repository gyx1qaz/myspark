package com.main.sparksubmit;

import org.apache.spark.launcher.SparkAppHandle;
import org.apache.spark.launcher.SparkLauncher;

import java.io.IOException;

public class MyLauncher {

    public static void main(String[] args) throws IOException {
        SparkAppHandle handle = new SparkLauncher()
                .setAppResource("/my/app.jar")
                .setMainClass("my.spark.app.Main")
                .setMaster("local")
                .setConf(SparkLauncher.DRIVER_MEMORY, "2g")
                .startApplication();
        // Use handle API to monitor / control application.
    }
}
