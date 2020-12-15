package mr;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * @author eren
 *
 * Reducer<KEYIN,VALUEIN,KEYOUT,VALUEOUT>
 *     KEYIN map阶段输出的key
 *     VALUEIN map阶段输出的value
 *     KEYOUT 最终结果的单词
 *     VALUEOUT 最终单词的次数
 */
public class WordReducer extends Reducer<Text, LongWritable, Text, LongWritable> {

    @Override
    protected void reduce(Text key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {
        //1.定义一个统计的变量
        long count = 0;
        //2.迭代累加
        for (LongWritable value : values) {
            count += value.get();
        }
        //3.写入上下文
        context.write(key, new LongWritable(count));
    }
}
