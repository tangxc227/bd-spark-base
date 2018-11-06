package com.tangxc.spark.lesson01.secondarysort;

import org.apache.spark.Partitioner;
import scala.Tuple2;

/**
 * @Author: tangxc
 * @Description:
 * @Date: Created in 10:15 2018/11/6
 * @Modified by:
 */
public class CustomerPartitioner extends Partitioner {

    private static final long serialVersionUID = 3517939181151604350L;

    private final int numPartitions;

    public CustomerPartitioner(int numPartitions) {
        assert (numPartitions > 0);
        this.numPartitions = numPartitions;
    }

    @Override
    public int numPartitions() {
        return numPartitions;
    }

    @Override
    public int getPartition(Object key) {
        if (null == key) {
            return 0;
        } else if (key instanceof Tuple2) {
            Tuple2<String, Integer> tuple2 = (Tuple2<String, Integer>) key;
            return Math.abs(tuple2._1.hashCode() % numPartitions);
        } else {
            return Math.abs(key.hashCode() % numPartitions);
        }
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        CustomerPartitioner that = (CustomerPartitioner) o;
        return numPartitions == that.numPartitions;
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + numPartitions;
        return result;
    }

}
