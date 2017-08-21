package com.yangyz.utils;

import com.yangyz.Exception.Counter;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.util.HashMap;
import java.util.Iterator;
import java.util.TreeMap;

/**
 * Created by yangyz on 2017/8/20.
 */
public class HadoopUtils {

    public static TreeMap getSortedData(Iterable<Text> values, Reducer.Context context)
    {
        TreeMap<Long,String> tm=new TreeMap<Long,String> ();
        //遍历values
        for(Text v:values){ //v:pageid unix
            String[] kv=v.toString().split(",");
            try{

                tm.put(Long.parseLong(kv[1]),kv[0]);
            }catch(NumberFormatException e)
            {
                context.getCounter(Counter.TIMESKIP).increment(1);
            }


        }
        return tm;

    }
    public static HashMap calcStayTime(TreeMap<Long,String> tm)
    {
        HashMap<String,Float> hm=new HashMap<String,Float>();
        //得到时间的迭代器
        Iterator<Long> iter = tm.keySet().iterator();
        Long currentTime=iter.next();
        while(iter.hasNext())
        {
           Long nextTime=iter.next();
            Float diff = (nextTime - currentTime) / 60.0f;
           if(diff<=60.0)
           {
               String posCurrent=tm.get(currentTime);
               if(!hm.containsKey(posCurrent))//如果不存在这个key
               {
                   hm.put(posCurrent,diff);
               }else{
                   hm.put(posCurrent,hm.get(posCurrent)+diff);
               }
           }
            currentTime=nextTime;
        }
        return hm;

    }
}
