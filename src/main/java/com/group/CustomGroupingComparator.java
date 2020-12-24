package com.group;


import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class CustomGroupingComparator extends WritableComparator {

    // 将我们自定义的OrderBean注册到我们自定义的CustomGroupingComparator当中
    // 表示我们的分组器在分组的时候，对OrderBean这一种类型的数据进行分组
    // 传入作为key的bean的class类型，以及制定需要让框架做反射获取实例对象
    public CustomGroupingComparator() {
        super(OrderBean.class, true);
    }

    @Override
    public int compare(WritableComparable a, WritableComparable b) {
        OrderBean first = (OrderBean) a;
        OrderBean second = (OrderBean) b;
        int i = first.getOrderId().compareTo(second.getOrderId());
        return i;
    }
}
