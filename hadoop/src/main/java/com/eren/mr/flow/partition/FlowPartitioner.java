package com.eren.mr.flow.partition;

import com.eren.mr.flow.FlowBean;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

/**
 * 按照手机号前缀分区
 *
 * @Author eren
 * @Date 2020/12/19 下午 01:31
 */
public class FlowPartitioner extends Partitioner<Text, FlowBean> {

    @Override
    public int getPartition(Text text, FlowBean flowBean, int numPartitions) {
        if (text.toString().startsWith("135")) {
            return 0;
        }
        if (text.toString().startsWith("136")) {
            return 1;
        }
        if (text.toString().startsWith("137")) {
            return 2;
        }
        return 3;
    }
}
