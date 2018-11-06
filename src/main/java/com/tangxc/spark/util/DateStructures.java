package com.tangxc.spark.util;

import java.util.SortedMap;

/**
 * @Author: tangxc
 * @Description:
 * @Date: Created in 09:51 2018/11/6
 * @Modified by:
 */
public class DateStructures {

    /**
     * 将较小的SortedMap向较大的合并
     * @param smaller
     * @param larger
     * @return
     */
    public static SortedMap<Integer, Integer> merge(final SortedMap<Integer, Integer> smaller,
                                                    final SortedMap<Integer, Integer> larger) {
        for (Integer key : smaller.keySet()) {
            Integer valueFromLargeMap = larger.get(key);
            if (null == valueFromLargeMap) {
                larger.put(key, smaller.get(key));
            } else {
                int mergedValue = valueFromLargeMap + smaller.get(key);
                larger.put(key, mergedValue);
            }
        }
        return larger;
    }

}
