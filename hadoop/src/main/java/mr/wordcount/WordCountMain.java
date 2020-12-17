package mr.wordcount;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;

/**
 * @author eren
 *
 * 主类，串联mapper和reducer并提供程序的入口
 */
public class WordCountMain {

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration entries = new Configuration();
        //一、初始化一个job对象
        Job job = Job.getInstance(entries, "WordCount");

        //2、设置job对象相关信息
        //2.1设置输入路径，让程序找到源文件位置
        job.setInputFormatClass(TextInputFormat.class);
        TextInputFormat.addInputPath(job, new Path("hdfs://192.168.248.130:8020/wcinput/wordcount.txt"));

        //2.2设置mapper类型，并设置k2，v2
        job.setMapperClass(WordMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(LongWritable.class);

        //2.3 4 5 6都是shuffle步骤，现在使用默认的就可以

        //2.7设置reducer类型，设置k3，v3
        job.setReducerClass(WordReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(LongWritable.class);

        //2.8设置输出的路径，保存结果
        job.setOutputFormatClass(TextOutputFormat.class);
        TextOutputFormat.setOutputPath(job, new Path("hdfs://192.168.248.130:8020/wordout"));

        //三、等待job执行完成
        boolean b = job.waitForCompletion(true);
        System.out.println(b);
        System.exit(b ? 0 : 1);
    }

}
