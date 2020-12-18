package com.eren.mr.flow.sort;

import com.eren.mr.flow.FlowBean;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * 排序mapper，实现对原始结果进行流量求和并排序
 *
 * @Author eren
 * @Date 2020/12/18 下午 10:50
 */
public class SortMapper extends Mapper<LongWritable, Text, FlowSortBean, FlowBean> {

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        //获取数据，根据空格分
        String[] splits = value.toString().split(" ");

        //组装数据
        FlowBean flowBean = new FlowBean();
        flowBean.setUpFlow(Integer.parseInt(splits[1]));
        flowBean.setDownFlow(Integer.parseInt(splits[2]));
        flowBean.setUpCountFlow(Integer.parseInt(splits[3]));
        flowBean.setDownCountFlow(Integer.parseInt(splits[4]));

        FlowSortBean flowSortBean = new FlowSortBean();
        flowSortBean.setPhone(splits[0]);
        flowSortBean.setUpFlow(Integer.parseInt(splits[1]));

        //写入上下文
        context.write(flowSortBean, flowBean);
    }
}
