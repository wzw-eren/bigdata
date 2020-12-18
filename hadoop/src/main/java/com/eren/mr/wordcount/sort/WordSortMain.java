package com.eren.mr.wordcount.sort;

import com.eren.mr.wordcount.WordMapper;
import com.eren.mr.wordcount.WordPartitioner;
import com.eren.mr.wordcount.WordReducer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;

public class WordSortMain {

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration entries = new Configuration();
        //一、初始化一个job对象
        Job job = Job.getInstance(entries, "WordCountSort");

        //二、设置job对象相关信息
        //2.1设置输入路径，让程序找到源文件位置
        job.setInputFormatClass(TextInputFormat.class);
        TextInputFormat.addInputPath(job, new Path("hdfs://192.168.248.130:8020/test/mkdirs/sort.txt"));

        //2.2设置mapper类型，并设置k2，v2
        job.setMapperClass(SortMapper.class);
        job.setMapOutputKeyClass(WordBean.class);
        job.setMapOutputValueClass(NullWritable.class);

        //2.3设置分区

        //2.4

        //2.5

        //2.6

        //2.7设置reducer类型，设置k3，v3
        job.setReducerClass(SortReducer.class);
        job.setOutputKeyClass(WordBean.class);
        job.setOutputValueClass(NullWritable.class);

        //2.8设置输出的路径，保存结果
        job.setOutputFormatClass(TextOutputFormat.class);
        TextOutputFormat.setOutputPath(job, new Path("hdfs://192.168.248.130:8020/wordout/sort1"));

        //三、等待job执行完成
        boolean b = job.waitForCompletion(true);
        System.out.println(b);
        System.exit(b ? 0 : 1);
    }
}
