package com.eren.mr.wordcount;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

/**
 * 单词统计partitioner
 *
 * @author eren
 * Partitioner<KEY, VALUE>
 *     KEY 单词的类型
 *     VALUE  单词的次数
 */
public class WordPartitioner extends Partitioner<Text, LongWritable> {

    public int getPartition(Text text, LongWritable longWritable, int numPartitions) {
        //分区的规则为，返回多少个int，就有多少分区
        //根据单词长度判断，长度>=5的在一个结果文件中，长度<5的在一个文件中
        if (text.toString().length() <= 5) {
            return 0;
        }
        return 1;
    }
}
