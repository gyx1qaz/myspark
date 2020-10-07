package com.main;

import org.apache.spark.launcher.SparkLauncher;

import java.io.IOException;
import java.util.HashMap;

public class LauncherApp {
    public static void main(String[] args) throws IOException, InterruptedException {

        HashMap env = new HashMap();
        //这两个属性必须设置
        env.put("HADOOP_CONF_DIR","/usr/local/hadoop/etc/overriterHaoopConf");
        env.put("JAVA_HOME","/usr/local/java/jdk1.8.0_151");
        //env.put("YARN_CONF_DIR","");

        SparkLauncher handle = new SparkLauncher(env)
                .setSparkHome("/usr/local/spark")
                .setAppResource("/usr/local/spark/spark-demo.jar")
                .setMainClass("com.learn.spark.SimpleApp")
                .setMaster("yarn")
                .setDeployMode("cluster")
                .setConf("spark.app.id", "11222")
                .setConf("spark.driver.memory", "2g")
                .setConf("spark.akka.frameSize", "200")
                .setConf("spark.executor.memory", "1g")
                .setConf("spark.executor.instances", "32")
                .setConf("spark.executor.cores", "3")
                .setConf("spark.default.parallelism", "10")
                .setConf("spark.driver.allowMultipleContexts","true")
                .setVerbose(true);


        Process process =handle.launch();
        InputStreamReaderRunnable inputStreamReaderRunnable = new InputStreamReaderRunnable(process.getInputStream(), "input");
        Thread inputThread = new Thread((Runnable) inputStreamReaderRunnable, "LogStreamReader input");
        inputThread.start();

        InputStreamReaderRunnable errorStreamReaderRunnable = new InputStreamReaderRunnable(process.getErrorStream(), "error");
        Thread errorThread = new Thread((Runnable) errorStreamReaderRunnable, "LogStreamReader error");
        errorThread.start();

        System.out.println("Waiting for finish...");
        int exitCode = process.waitFor();
        System.out.println("Finished! Exit code:" + exitCode);

    }

}
