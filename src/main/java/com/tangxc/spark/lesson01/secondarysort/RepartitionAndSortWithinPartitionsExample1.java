package com.tangxc.spark.lesson01.secondarysort;

import org.apache.spark.Partitioner;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

/**
 * @Author: tangxc
 * @Description:
 * @Date: Created in 17:03 2018/11/5
 * @Modified by:
 */
public class RepartitionAndSortWithinPartitionsExample1 implements Serializable {

    private static final long serialVersionUID = 7990498934492070786L;

    public static void main(String[] args) {
        JavaSparkContext jsc = getJavaSparkContext("SecondarySort");
        List<Tuple2<Integer, Integer>> paris = buildSampleList();
        JavaPairRDD<Integer, Integer> rdd = jsc.parallelizePairs(paris);
        Partitioner partitioner = new Partitioner() {
            @Override
            public int getPartition(Object key) {
                return (Integer) key % 3;
            }

            @Override
            public int numPartitions() {
                return 3;
            }
        };
        JavaPairRDD<Integer, Integer> repartitioned = rdd.repartitionAndSortWithinPartitions(partitioner);
        List<List<Tuple2<Integer, Integer>>> partitions = repartitioned.glom().collect();

        System.out.println("partition: 0 : " + partitions.get(0));
        System.out.println("partition: 1 : " + partitions.get(1));
        System.out.println("partition: 2 : " + partitions.get(2));

        jsc.close();
    }

    static List<Tuple2<Integer, Integer>> buildSampleList() {
        List<Tuple2<Integer, Integer>> pairs = new ArrayList<>();
        pairs.add(new Tuple2<>(6, 16));
        pairs.add(new Tuple2<>(1, 11));
        pairs.add(new Tuple2<>(2, 12));

        pairs.add(new Tuple2<>(3, 13));
        pairs.add(new Tuple2<>(4, 14));
        pairs.add(new Tuple2<>(5, 15));

        pairs.add(new Tuple2<>(0, 20));
        pairs.add(new Tuple2<>(7, 17));
        pairs.add(new Tuple2<>(8, 18));
        return pairs;
    }

    static JavaSparkContext getJavaSparkContext(String appName) {
        SparkConf conf = new SparkConf()
                .setMaster("local")
                .setAppName(appName);
        return new JavaSparkContext(conf);

    }

}
