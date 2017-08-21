package com.yangyz.ubt;

import com.yangyz.driver.BaseDriver;
import com.yangyz.driver.JobInitModel;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;

import java.io.IOException;

/**
 * Created by yangyz on 2017/8/21.
 */
public class UserStayTime {


    public static void main(String[] args) throws InterruptedException, IOException, ClassNotFoundException {
        if(args.length<2)
        {
            System.out.println("paramers is not mismatch");
            return ;
        }
        Configuration conf = new Configuration();
        conf.set("date",args[0]);
        conf.set("timepoint",args[1]);
        String[] inPath=new String[]{"/tmp/ubt_pageview_userStayTime/*"};
        String outPath="/tmp/ubt_pageview_userStayTime_outPut";
        String jobName="ubtUserStayTime";
        JobInitModel job = new JobInitModel(inPath, outPath, conf, null, jobName
                , UserStayTime.class, null, Mapper.class, Text.class, Text.class, null, null, Reducer.class
                , NullWritable.class, Text.class);
        BaseDriver.initJob(new JobInitModel[]{job});




    }


}
