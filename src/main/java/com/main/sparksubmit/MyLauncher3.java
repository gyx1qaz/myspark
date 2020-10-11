package com.main.sparksubmit;


import org.apache.spark.launcher.SparkLauncher;

import java.io.IOException;

/**
 * @function spark -cluster
 */
public class MyLauncher3 {
    public static void main(String[] args) throws IOException, InterruptedException {
        // 流式计算
        Process spark = new SparkLauncher()
                .setAppResource("/my/app.jar")
                .setMainClass("my.spark.app.Main")
                .setMaster("local")
                .setConf(SparkLauncher.DRIVER_MEMORY, "2g")
                .setDeployMode("cluster")
                .launch();
        spark.waitFor();
    }
}
