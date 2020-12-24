package com.example;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

// 必须实现 Writable 接口
public class SpeakBean implements Writable {

    // 反序列化时，需要反射调⽤空参构造函数，所以必须有空参构造
    public SpeakBean() {
    }

    public SpeakBean(long selfDuration, long thirdPartDuration) {
        this.selfDuration = selfDuration;
        this.thirdPartDuration = thirdPartDuration;
        this.sumDuration = this.selfDuration + this.thirdPartDuration;
    }

    private long selfDuration; // ⾃有内容时⻓(秒)
    private long thirdPartDuration; // 第三⽅内容时⻓
    private long sumDuration; // 总时⻓


    // 序列化
    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeLong(selfDuration);
        dataOutput.writeLong(thirdPartDuration);
        dataOutput.writeLong(sumDuration);
    }

    // 反序列化⽅法读顺序必须和写序列化⽅法的写顺序必须⼀致
    @Override
    public void readFields(DataInput dataInput) throws IOException {
        this.selfDuration = dataInput.readLong();
        this.thirdPartDuration = dataInput.readLong();
        this.sumDuration = dataInput.readLong();
    }

    // toString⽅法，⽅便后续打印到⽂本
    @Override
    public String toString() {
        return selfDuration +
                "\t" + thirdPartDuration +
                "\t" + sumDuration ;
    }

    public long getSelfDuration() {
        return selfDuration;
    }

    public void setSelfDuration(long selfDuration) {
        this.selfDuration = selfDuration;
    }

    public long getThirdPartDuration() {
        return thirdPartDuration;
    }

    public void setThirdPartDuration(long thirdPartDuration) {
        this.thirdPartDuration = thirdPartDuration;
    }

    public long getSumDuration() {
        return sumDuration;
    }

    public void setSumDuration(long sumDuration) {
        this.sumDuration = sumDuration;
    }
}