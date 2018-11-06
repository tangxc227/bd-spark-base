package com.tangxc.spark.lesson01.secondarysort;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

import java.io.Serializable;

/**
 * @Author: tangxc
 * @Description:
 * @Date: Created in 09:59 2018/11/6
 * @Modified by:
 */
public class SecondarySortUsingRepartitionAndSortWithinPartitions implements Serializable {

    private static final long serialVersionUID = -95531861282437120L;

    public static void main(String[] args) {
        if (args.length != 3) {
            System.err.println("Usage <number-of-partitions> <input-dir> <output-dir>");
            System.exit(1);
        }
        int partitions = Integer.valueOf(args[0]);
        String inputPath = args[1];
        String outputPath = args[2];

        SparkConf conf = new SparkConf()
                .setAppName("SecondarySort");
        JavaSparkContext jsc = new JavaSparkContext(conf);

        JavaRDD<String> inputRDD = jsc.textFile(inputPath);

        JavaPairRDD<Tuple2<String, Integer>, Integer> valueToKey = inputRDD.mapToPair(new PairFunction<String, Tuple2<String, Integer>, Integer>() {

            private static final long serialVersionUID = 2222190844785746534L;

            @Override
            public Tuple2<Tuple2<String, Integer>, Integer> call(String line) throws Exception {
                String[] columns = line.split(",");
                Integer value = Integer.valueOf(columns[3]);
                Tuple2<String, Integer> key = new Tuple2<>(columns[0] + "-" + columns[1], value);
                return new Tuple2<>(key, value);
            }

        });

        JavaPairRDD<Tuple2<String, Integer>, Integer> sorted = valueToKey.repartitionAndSortWithinPartitions(new CustomerPartitioner(partitions), TupleComparatorDescending.INSTANCE);

        JavaPairRDD<String, Integer> result = sorted.mapToPair(tuple -> new Tuple2<>(tuple._1._1, tuple._2));

        result.saveAsTextFile(outputPath);

        jsc.close();
    }

}
