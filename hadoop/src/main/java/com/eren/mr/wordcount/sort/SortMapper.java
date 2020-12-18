package com.eren.mr.wordcount.sort;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * 单词统计排序mapper
 *
 * @author eren
 */
public class SortMapper extends Mapper<LongWritable, Text, WordBean, NullWritable> {

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        //1.拆分数据
        String[] words = value.toString().split(" ");

        //2.将对应的值给传入到实例对象中
        WordBean wordBean = new WordBean();
        wordBean.setWord(words[0]);
        wordBean.setNum(Integer.parseInt(words[1]));

        //3.写入上下文
        context.write(wordBean, NullWritable.get());
    }
}
