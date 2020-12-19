package com.eren.mr.flow.partition;

import com.eren.mr.flow.FlowBean;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.UUID;

/**
 * 手机号分区mapper
 *
 * @Author eren
 * @Date 2020/12/19 下午 01:52
 */
public class PartitionerMapper extends Mapper<LongWritable, Text, Text, FlowBean> {

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        //获取相应的数据
        String[] split = value.toString().split("\t");

        FlowBean flow = new FlowBean();
        //封装bean对象
        flow.setUpFlow(Integer.parseInt(split[6]));
        flow.setDownFlow(Integer.parseInt(split[7]));
        flow.setUpCountFlow(Integer.parseInt(split[8]));
        flow.setDownCountFlow(Integer.parseInt(split[9]));

        //写入上下文
        context.write(new Text(split[1]), flow);
    }
}
