package com.tangxc.spark.lesson01.secondarysort;

import scala.Tuple2;

import java.io.Serializable;
import java.util.Comparator;

/**
 * @Author: tangxc
 * @Description:
 * @Date: Created in 16:17 2018/11/6
 * @Modified by:
 */
public class SparkTupleComparator implements Comparator<Tuple2<Integer, Integer>>, Serializable {

    private static final long serialVersionUID = 5262329694398182638L;

    public static final SparkTupleComparator INSTANCE = new SparkTupleComparator();

    @Override
    public int compare(Tuple2<Integer, Integer> o1, Tuple2<Integer, Integer> o2) {
        return o1._1.compareTo(o2._1);
    }

}
