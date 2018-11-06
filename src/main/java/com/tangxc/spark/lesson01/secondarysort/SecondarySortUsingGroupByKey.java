package com.tangxc.spark.lesson01.secondarysort;

import com.tangxc.spark.util.SparkUtil;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * @Author: tangxc
 * @Description:
 * @Date: Created in 15:29 2018/11/6
 * @Modified by:
 */
public class SecondarySortUsingGroupByKey {

    public static void main(String[] args) throws Exception {
        if (args.length < 2) {
            System.err.println("Usage: SecondarySortUsingGroupByKey <input> <output>");
            System.exit(1);
        }
        String inputPath = args[0];
        System.out.println("inputPath = " + inputPath);
        String outputPath = args[1];
        System.out.println("outputPath = " + outputPath);

        JavaSparkContext jsc = SparkUtil.createJavaSparkContext("SecondarySortUsingGroupByKey");

        JavaRDD<String> lines = jsc.textFile(inputPath, 1);

        JavaPairRDD<String, Tuple2<Integer, Integer>> pairs = lines.mapToPair(line -> {
           String[] tokens = line.split(",");
            System.err.println(tokens[0] + "," + tokens[1] + "," + tokens[2]);
            Tuple2<Integer, Integer> timevalue = new Tuple2<>(Integer.valueOf(tokens[1]), Integer.valueOf(tokens[2]));
            return new Tuple2<>(tokens[0], timevalue);
        });

        JavaPairRDD<String, Iterable<Tuple2<Integer, Integer>>> groups = pairs.groupByKey();

        JavaPairRDD<String, Iterable<Tuple2<Integer, Integer>>> sorted = groups.mapValues(new Function<Iterable<Tuple2<Integer,Integer>>, Iterable<Tuple2<Integer,Integer>>>() {

            private static final long serialVersionUID = -7499595737262966868L;

            @Override
            public Iterable<Tuple2<Integer, Integer>> call(Iterable<Tuple2<Integer, Integer>> v1) throws Exception {
                List<Tuple2<Integer, Integer>> newList = new ArrayList<>(iterableToList(v1));
                Collections.sort(newList, SparkTupleComparator.INSTANCE);
                return newList;
            }

            public List<Tuple2<Integer, Integer>> iterableToList(Iterable<Tuple2<Integer,Integer>> iterable) {
                List<Tuple2<Integer,Integer>> list = new ArrayList<>();
                for (Tuple2<Integer,Integer> item : iterable) {
                    list.add(item);
                }
                return list;
            }

        });

        List<Tuple2<String, Iterable<Tuple2<Integer, Integer>>>> output3 = sorted.collect();
        for (Tuple2<String, Iterable<Tuple2<Integer, Integer>>> t : output3) {
            Iterable<Tuple2<Integer, Integer>> list = t._2;
            System.out.println(t._1);
            for (Tuple2<Integer, Integer> t2 : list) {
                System.out.println(t2._1 + "," + t2._2);
            }
            System.out.println("=====");
        }

        sorted.saveAsTextFile(outputPath);

        System.exit(0);

        jsc.close();
    }

}
