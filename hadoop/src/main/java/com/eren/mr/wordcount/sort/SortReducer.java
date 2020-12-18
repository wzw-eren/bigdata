package com.eren.mr.wordcount.sort;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * 单词统计排序reducer
 *
 * @author eren
 */
public class SortReducer extends Reducer<WordBean, NullWritable, WordBean, NullWritable> {

    @Override
    protected void reduce(WordBean key, Iterable<NullWritable> values, Context context) throws IOException, InterruptedException {
        //将map阶段输出的结果拿过来进行汇总
        context.write(key, NullWritable.get());
    }
}
