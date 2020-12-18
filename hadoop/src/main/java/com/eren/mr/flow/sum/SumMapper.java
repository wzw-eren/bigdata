package com.eren.mr.flow.sum;

import com.eren.mr.flow.FlowBean;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * 流量求和mapper
 * Mapper<KEYIN, VALUEIN, KEYOUT, VALUEOUT>
 *     KEYIN 输入偏移量
 *     VALUEIN 一行文本
 *     KEYOUT 手机号
 *     VALUEOUT flowbean
 *
 * @Author eren
 * @Date 2020/12/18 下午 09:41
 */
public class SumMapper extends Mapper<LongWritable, Text, Text, FlowBean> {

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        //从文件中获取到想要的数据,这里是根据制表符切分
        String[] split = value.toString().split("\t");
        String phone = split[1];

        //封装bean对象
        FlowBean flow = new FlowBean();
        flow.setUpFlow(Integer.parseInt(split[6]));
        flow.setDownFlow(Integer.parseInt(split[7]));
        flow.setUpCountFlow(Integer.parseInt(split[8]));
        flow.setDownCountFlow(Integer.parseInt(split[9]));

        //写入上下文
        context.write(new Text(phone), flow);

    }
}
