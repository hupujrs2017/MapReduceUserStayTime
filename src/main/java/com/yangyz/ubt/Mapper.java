package com.yangyz.ubt;

import com.yangyz.Exception.Counter;
import com.yangyz.utils.LineFormater;
import org.apache.hadoop.io.Text;
import com.yangyz.Exception.LineException;

import java.io.IOException;

/**
 * Created by yangyz on 2017/8/21.
 */
public class Mapper  extends org.apache.hadoop.mapreduce.Mapper<Object, Text, Text, Text>{

    //进行数据格式化操作
    LineFormater lineFormater = new LineFormater();
    //要计算的日期
    String date;
    //要计算的时段
    String[] timepoint;
    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        this.date=context.getConfiguration().get("date");
        this.timepoint=context.getConfiguration().get("timepoint").split("-");
    }

    @Override
    protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        try {
            lineFormater.format(value.toString(),this.date,this.timepoint);
        } catch (LineException e) {
            if (e.getFlag() == -1) {
                context.getCounter(Counter.OUTOFTIMESKIP).increment(1);
            } else if (e.getFlag() == 0) {
                context.getCounter(Counter.TIMESKIP).increment(1);
            } else {
                context.getCounter(Counter.OUTOFTIMEFLASGSKIP).increment(1);
            }
        }
        context.write(lineFormater.outKey(),lineFormater.outValue());
    }

}
