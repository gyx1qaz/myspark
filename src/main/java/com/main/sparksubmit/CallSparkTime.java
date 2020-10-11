package com.main.sparksubmit;


import java.io.IOException;
import org.apache.spark.launcher.SparkAppHandle;
import org.apache.spark.launcher.SparkLauncher;


//此段code参考链接：
//https://blog.csdn.net/Yang838020787/article/details/89838551
public class CallSparkTime {

    public void submitJob(String queryType, String start, String stop) throws IOException {

        SparkAppHandle handler = new SparkLauncher()
                .setAppName("SparkTimeCount")
                .setSparkHome("/opt/spark")//spark安装目录
                .setMaster("yarn") // 提交方式
                .setConf("spark.driver.memory", "1g")
                .setConf("spark.executor.memory", "1g")
                .setConf("spark.executor.cores", "2")
                .setAppResource("/root/test/demo.jar")//jar包目录
                .setMainClass("kafkaDemo.testKafka.sparkTest.SparkTimeCount")//提交的类
                .setDeployMode("cluster")
                .addAppArgs(queryType, start, stop)//提交的参数
                .startApplication(new SparkAppHandle.Listener() {

                    @Override
                    public void stateChanged(SparkAppHandle sparkAppHandle) {
                        System.out.println("state  changed ");
                    }

                    @Override
                    public void infoChanged(SparkAppHandle sparkAppHandle) {
                        System.out.println("info  changed ");
                    }
                });

        while (!"FINISHED".equalsIgnoreCase(handler.getState().toString())
                && !"FAILED".equalsIgnoreCase(handler.getState().toString())) {
            System.out.println("id    " + handler.getAppId());
            System.out.println("state " + handler.getState());


            try {
                Thread.sleep(10000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }

}
