package mr.wordcount;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * @author erenwu
 * Mapper<KEYIN, VALUEIN, KEYOUT, VALUEOUT>
 *     KEYIN 是指框架读取到的数据集的key的类型，在默认情况下，读取到key就是一行的数据相对于文本开头的偏移量。key的类型可以为Long吗？
 *     VALUEIN 是指框架读取到的数据集的value的类型，在默认情况下，读取到value就是一行数据。value可以为String吗？
 *     KEYOUT 是指用户自定义的逻辑方法返回值的key的类型，由用户根据业务逻辑自己决定。可以为String吗？
 *     VALUEOUT 是指用户自定义的逻辑方法返回值的value的类型，由用户根据业务逻辑自己决定的，在word count中就是次数，可以为Long吗？
 *
 * String、Long都是JDK的数据类型，在序列化的时候，效率低
 * hadoop自定义了一些数据类型，在序列化的时候能大大提高效率，在hadoop程序中，需要使用hadoop的数据类型。
 * Long -> LongWritable
 * String -> Text
 * Integer -> IntWritable
 * Null -> NullWritable
 */
public class WordMapper extends Mapper<LongWritable, Text, Text, LongWritable> {

    /**
     *
     * @param key 偏移量
     * @param value 一行文本数据
     * @param context 上下文
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        //1.单词切分
        String[] words = value.toString().split(" ");
        //2.计数一次，帮单词转化成key-value键值对
        for (String word : words) {
            //3.写入上下文
            context.write(new Text(word), new LongWritable(1));
        }

    }
}
