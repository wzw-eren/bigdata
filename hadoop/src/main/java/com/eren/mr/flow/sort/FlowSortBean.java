package com.eren.mr.flow.sort;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * 排序
 *
 * @Author eren
 * @Date 2020/12/18 下午 10:36
 */
public class FlowSortBean implements WritableComparable<FlowSortBean> {

    private String phone;

    private Integer upFlow;

    public String getPhone() {
        return phone;
    }

    public void setPhone(String phone) {
        this.phone = phone;
    }

    public Integer getUpFlow() {
        return upFlow;
    }

    public void setUpFlow(Integer upFlow) {
        this.upFlow = upFlow;
    }

    /**
     * 降序排序
     *
     * @param o
     * @return
     */
    @Override
    public int compareTo(FlowSortBean o) {
        return o.getUpFlow() - this.upFlow;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeInt(this.upFlow);
        out.writeUTF(this.phone);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        this.upFlow = in.readInt();
        this.phone = in.readUTF();
    }
}
