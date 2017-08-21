package com.yangyz.utils;

import com.yangyz.Exception.LineException;
import org.apache.hadoop.io.Text;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * Created by yangyz on 2017/8/21.
 */
public class LineFormater {
    private String vid;
    private String pageid;
    private String time;
    private String timeFlag;
    private Date day;
    private SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");



    public void format(String value,String date,String timepoint[]) throws LineException{
        String[] words=value.split("\t");

            this.vid = words[0];
            this.pageid = words[1];
            this.time = words[2];

        //如果不是当前日期的数据,则过滤并统计异常 只取参数中的日期
        if (!this.time.startsWith(date)) {
            throw new LineException("Incorrect datetime!", -1);
        }
        //将字符串的time字段转成时间类型的day字段,如果该时间格式不正确,则过滤并统计异常
        try {
            this.day = this.simpleDateFormat.parse(this.time);
        } catch (ParseException e) {
            throw new LineException("Incorrect date format!", 0);
        }
        Integer hour=Integer.parseInt(this.time.split(" ")[1].split(":")[0]);
        //遍历时间段
        for(int i=0;i<timepoint.length;i++)
        {
            if(hour<Integer.parseInt(timepoint[i]))
            {
                if(i==0)
                {
                    timeFlag="00"+"-"+timepoint[i];
                }
                else
                {
                    timeFlag=timepoint[i-1]+"-"+timepoint[i];
                }
                break;
            }else
            {
                try{
                    timeFlag=timepoint[i]+"-"+Integer.parseInt(timepoint[i+1]);
                }catch(Exception e){
                    throw new LineException("Current hour is bigger than the max-timepoint!", 1);
                }

            }


        }

    }

    public Text outKey(){
        return new Text(this.vid+","+this.timeFlag);
    }
    public Text outValue(){
        return new Text(this.pageid+","+ this.day.getTime() / 1000L);
    }
}
