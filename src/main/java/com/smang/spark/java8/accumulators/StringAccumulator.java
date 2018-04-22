package com.smang.spark.java8.accumulators;

import org.apache.commons.collections.ListUtils;
import org.apache.spark.AccumulatorParam;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

/*
1.Computations inside transformations are evaluated lazily, so unless an action happens on an RDD the transformationsare not executed. As a result of this, accumulators used inside functions like map() or filter() wont get executed unless some action happen on the RDD.
Spark guarantees to update accumulators inside actionsonly once. So even if a task is restarted and the lineage is recomputed, the accumulators will be updated only once.
2.Spark does not guarantee this for transformations. So if a task is restarted and the lineage is recomputed, there are chances of undesirable side effects when the accumulators will be updated more than once.
To be on the safe side, always use accumulators inside actions ONLY.
*/

public class StringAccumulator implements AccumulatorParam<List<String>>, Serializable {
   @Override
    public List<String> addAccumulator(List<String> t1, List<String> t2) {
       return ListUtils.union(t1,t2);
    }

    @Override
    public List<String> addInPlace(List<String> r1, List<String> r2) {
        return ListUtils.union(r1,r2);
    }

    @Override
    public List<String> zero(List<String> initialValue) {
        return new ArrayList<String>();
    }
}
