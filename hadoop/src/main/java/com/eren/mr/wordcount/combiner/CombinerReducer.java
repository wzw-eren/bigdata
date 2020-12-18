package com.eren.mr.wordcount.combiner;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * combiner案例，在单词统计的基础上增加combiner逻辑。将word mapper输出的统计进行合并，再传入reducer
 *
 * @author eren
 */
public class CombinerReducer extends Reducer<Text, LongWritable, Text, LongWritable> {

    @Override
    protected void reduce(Text key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {
        //定义一个变量
        long count = 0;

        //累加次数
        for (LongWritable value : values) {
            count = value.get() + count;
        }

        //写入上下文
        context.write(key, new LongWritable(count));
    }
}

