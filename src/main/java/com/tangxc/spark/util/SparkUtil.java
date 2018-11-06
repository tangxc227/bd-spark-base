package com.tangxc.spark.util;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;

/**
 * @Author: tangxc
 * @Description:
 * @Date: Created in 09:42 2018/11/6
 * @Modified by:
 */
public class SparkUtil {

    /**
     *
     * @return
     * @throws Exception
     */
    public static JavaSparkContext createJavaSparkContext() throws Exception {
        return new JavaSparkContext();
    }

    public static JavaSparkContext createJavaSparkContext(String sparkMasterURL, String applicationName) throws Exception {
        return new JavaSparkContext(sparkMasterURL, applicationName);
    }

    public static JavaSparkContext createJavaSparkContext(String applicationName) throws Exception {
        SparkConf conf = new SparkConf()
                .setAppName(applicationName);
        return new JavaSparkContext(conf);
    }

    public static String version() {
        return "2.0.0";
    }

}
