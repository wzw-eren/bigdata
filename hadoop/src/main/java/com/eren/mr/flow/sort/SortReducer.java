package com.eren.mr.flow.sort;

import com.eren.mr.flow.FlowBean;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * 排序reducer
 *
 * @Author eren
 * @Date 2020/12/18 下午 11:00
 */
public class SortReducer extends Reducer<FlowSortBean, FlowBean, Text, FlowBean> {

    @Override
    protected void reduce(FlowSortBean key, Iterable<FlowBean> values, Context context) throws IOException, InterruptedException {
        FlowBean flowBean = new FlowBean();
        for (FlowBean value : values) {
            flowBean = value;
        }
        //写入上下文
        context.write(new Text(key.getPhone()), flowBean);
    }
}
