package com.yangyz.Exception;

/**
 * Created by yangyz on 2017/8/21.
 */
public class LineException extends Exception{

    //异常的标识
    //-1:不是当前日期内
    //0:时间格式不正确
    //1:时间坐在的小时超出最大的时段
    private Integer flag;
    public LineException(String msg,int code)
    {
        super(msg);
        this.flag=code;
    }
    public Integer getFlag() {
        return flag;
    }
}
