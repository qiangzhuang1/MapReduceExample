package com.comparable;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

// 必须实现 WritableComparable 接口
public class ComparableBean implements WritableComparable<ComparableBean> {
    private String deviceId;//设备id
    private String appkey;//appkey合作硬件⼚商id
    private Long selfDuration; //⾃有内容时⻓
    public ComparableBean() {
    }
    public ComparableBean(String deviceId, String appkey, Long selfDuration) {
        this.deviceId = deviceId;
        this.appkey = appkey;
        this.selfDuration = selfDuration;
    }
    public String getDeviceId() {
        return deviceId;
    }
    public void setDeviceId(String deviceId) {
        this.deviceId = deviceId;
    }
    public String getAppkey() {
        return appkey;
    }
    public void setAppkey(String appkey) {
        this.appkey = appkey;
    }

    public Long getSelfDuration() {
        return selfDuration;
    }
    public void setSelfDuration(Long selfDuration) {
        this.selfDuration = selfDuration;
    }
    //序列化
    @Override
    public void write(DataOutput out) throws IOException {
        out.writeUTF(deviceId);
        out.writeUTF(appkey);
        out.writeLong(selfDuration);
    }
    //反序列化
    @Override
    public void readFields(DataInput in) throws IOException {
        this.deviceId = in.readUTF();
        this.appkey = in.readUTF();
        this.selfDuration = in.readLong();
    }
    @Override
    public String toString() {
        return deviceId + '\t' +
                appkey + '\t' +
                selfDuration;
    }

    @Override
    public int compareTo(ComparableBean o) {
        int result;
        // 按照⾃有内容时⻓，倒序排列
        if (selfDuration > o.getSelfDuration()) {
            result = -1;
        }else if (selfDuration < o.getSelfDuration()) {
            result = 1;
        }else {
            result = 0;
        }
        return result;
    }
}