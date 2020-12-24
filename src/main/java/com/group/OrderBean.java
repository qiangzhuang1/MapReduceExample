package com.group;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

// 必须实现 WritableComparable 接口
public class OrderBean implements WritableComparable<OrderBean> {
    private String orderId;
    private Double price;
    public OrderBean() {}

    //序列化
    @Override
    public void write(DataOutput out) throws IOException {
        out.writeUTF(orderId);
        out.writeDouble(price);
    }
    //反序列化
    @Override
    public void readFields(DataInput in) throws IOException {
        this.orderId = in.readUTF();
        this.price = in.readDouble();
    }

    @Override
    public int compareTo(OrderBean o) {
        // 如果订单id相同，则比较金额，金额大的排在前面
        int i = this.orderId.compareTo(o.orderId);
        if (i == 0) {
            i = -this.price.compareTo(o.price);
        }
        return i;
    }

    @Override
    public String toString() {
        return orderId + '\t' + price;
    }

    public String getOrderId() {
        return orderId;
    }

    public void setOrderId(String orderId) {
        this.orderId = orderId;
    }

    public Double getPrice() {
        return price;
    }

    public void setPrice(Double price) {
        this.price = price;
    }
}