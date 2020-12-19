package com.eren.mr.flow.partition;

import com.eren.mr.flow.FlowBean;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * 手机号分区reducer
 *
 * @Author eren
 * @Date 2020/12/19 下午 01:52
 */
public class PartitionerReducer extends Reducer<Text, FlowBean, Text, FlowBean> {

    @Override
    protected void reduce(Text key, Iterable<FlowBean> values, Context context) throws IOException, InterruptedException {

        for (FlowBean value : values) {
            //写入上下文
            context.write(key, value);
        }

    }
}
