package com.tangxc.spark.lesson01.secondarysort;

import scala.Tuple2;

import java.io.Serializable;
import java.util.Comparator;

/**
 * @Author: tangxc
 * @Description:
 * @Date: Created in 10:49 2018/11/6
 * @Modified by:
 */
public class TupleComparatorDescending implements Comparator<Tuple2<String, Integer>>, Serializable {

    private static final long serialVersionUID = -3447168696788667708L;

    static final TupleComparatorDescending INSTANCE = new TupleComparatorDescending();

    @Override
    public int compare(Tuple2<String, Integer> o1, Tuple2<String, Integer> o2) {
        int tmp = o2._1.compareTo(o1._1);
        if (tmp != 0) {
            return tmp;
        }
        return o2._2.compareTo(o1._2);
    }

}
