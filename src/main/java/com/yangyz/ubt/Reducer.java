package com.yangyz.ubt;

import com.yangyz.Exception.Counter;
import com.yangyz.utils.HadoopUtils;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.TreeMap;

/**
 * Created by yangyz on 2017/8/21.
 */
public class Reducer extends org.apache.hadoop.mapreduce.Reducer<Text, Text, NullWritable, Text>{

    //要计算的时间
    String day;
    Text out = new Text();
    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        super.setup(context);
        this.day=context.getConfiguration().get("date");
    }

    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        TreeMap<Long,String> tm= HadoopUtils.getSortedData(values,context);
        String vid=key.toString().split(",")[0];
        String timeFlag=key.toString().split(",")[1];
        //下面进行核心操作，相减  传入unix，pageid 返回pageid,unix
        SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        try{
            //设置该数据所在的最后时段的unixtime
            Date offTimeflag = simpleDateFormat.parse(this.day + " " + timeFlag.split("-")[1] + ":00:00");
            tm.put(offTimeflag.getTime() / 1000L, "OFF");
            HashMap<String, Float> resMap = HadoopUtils.calcStayTime(tm);
            for (Map.Entry<String, Float> entry : resMap.entrySet()) {
                String builder = vid + "|" +
                        timeFlag + "|" +
                        entry.getKey() + "|" +
                        entry.getValue();
                out.set(builder);
                context.write(NullWritable.get(), out);
            }
        }catch(ParseException e)
        {
            context.getCounter(Counter.TIMEFORMATERR).increment(1);
        }



    }
}
