package com.partition;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

// 必须实现 Writable 接口
public class PartitionBean implements Writable {
    private String deviceId;//设备id
    private String appkey;//appkey合作硬件⼚商id
    private Long selfDuration; //⾃有内容时⻓
    private Long thirdPartDuration; //第三⽅内容时⻓
    public PartitionBean() {
    }
    public PartitionBean(String deviceId, String appkey,Long selfDuration, Long thirdPartDuration) {
        this.deviceId = deviceId;
        this.appkey = appkey;
        this.selfDuration = selfDuration;
        this.thirdPartDuration = thirdPartDuration;
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
    public Long getThirdPartDuration() {
        return thirdPartDuration;
    }
    public void setThirdPartDuration(Long thirdPartDuration) {
        this.thirdPartDuration = thirdPartDuration;
    }
    //序列化
    @Override
    public void write(DataOutput out) throws IOException {
        out.writeUTF(deviceId);
        out.writeUTF(appkey);
        out.writeLong(selfDuration);
        out.writeLong(thirdPartDuration);
    }
    //反序列化
    @Override
    public void readFields(DataInput in) throws IOException {
        this.deviceId = in.readUTF();
        this.appkey = in.readUTF();
        this.selfDuration = in.readLong();
        this.thirdPartDuration = in.readLong();
    }
    @Override
    public String toString() {
        return deviceId + '\t' +
                appkey + '\t' +
                selfDuration +'\t'+
                thirdPartDuration;
    }
}