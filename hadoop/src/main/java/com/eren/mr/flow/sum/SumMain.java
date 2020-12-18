package com.eren.mr.flow.sum;

import com.eren.mr.flow.FlowBean;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;

/**
 * 统计手机流量topN
 *  1.统计每个手机号的数据包和流量总和
 *  2.将1中的结果按照upflow流量倒排
 *  3.按照手机号码分区
 *
 * @Author eren
 * @Date 2020/12/18 下午 09:09
 */
public class SumMain {

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration entries = new Configuration();
        //一、初始化一个job对象
        Job job = Job.getInstance(entries, "flowsum");

        //二、设置job对象相关信息
        //2.1设置输入路径，让程序找到源文件位置
        job.setInputFormatClass(TextInputFormat.class);
        TextInputFormat.addInputPath(job, new Path("hdfs://192.168.248.130:8020/test/mkdirs/flow.log"));

        //2.2设置mapper类型，并设置k2，v2
        job.setMapperClass(SumMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(FlowBean.class);

        //2.3设置分区

        //2.4

        //2.5设置combiner

        //2.6

        //2.7设置reducer类型，设置k3，v3
        job.setReducerClass(SumReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(FlowBean.class);

        //设置NumReduceTask个数，多少个分区，就需要多少个task
        //job.setNumReduceTasks(2);

        //2.8设置输出的路径，保存结果
        job.setOutputFormatClass(TextOutputFormat.class);
        TextOutputFormat.setOutputPath(job, new Path("hdfs://192.168.248.130:8020/flow/sum1"));

        //三、等待job执行完成
        boolean b = job.waitForCompletion(true);
        System.out.println(b);
        System.exit(b ? 0 : 1);
    }
}
