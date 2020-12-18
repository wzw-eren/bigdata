package com.eren.mr.flow;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * 手机流量实体类
 *
 * @Author eren
 * @Date 2020/12/18 下午 09:33
 */
public class FlowBean implements Writable {

    /**
     * 上行流量
     */
    private Integer upFlow;

    /**
     * 下行流量
     */
    private Integer downFlow;

    /**
     * 上行数据包
     */
    private Integer upCountFlow;

    /**
     * 下行数据包
     */
    private Integer downCountFlow;

    public Integer getUpFlow() {
        return upFlow;
    }

    public void setUpFlow(Integer upFlow) {
        this.upFlow = upFlow;
    }

    public Integer getDownFlow() {
        return downFlow;
    }

    public void setDownFlow(Integer downFlow) {
        this.downFlow = downFlow;
    }

    public Integer getUpCountFlow() {
        return upCountFlow;
    }

    public void setUpCountFlow(Integer upCountFlow) {
        this.upCountFlow = upCountFlow;
    }

    public Integer getDownCountFlow() {
        return downCountFlow;
    }

    public void setDownCountFlow(Integer downCountFlow) {
        this.downCountFlow = downCountFlow;
    }

    @Override
    public String toString() {
        return upFlow + " " + downFlow + " " + upCountFlow + " " + downCountFlow;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeInt(this.upFlow);
        out.writeInt(this.upCountFlow);
        out.writeInt(this.downCountFlow);
        out.writeInt(this.downFlow);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        this.upFlow = in.readInt();
        this.upCountFlow = in.readInt();
        this.downCountFlow = in.readInt();
        this.downFlow = in.readInt();
    }

}
