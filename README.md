# MapReduceUserStayTime
mapreduce程序，用于计算用户在各个页面停留时间

假设有一种场景，我们想知道用户在各个页面的停留时长，对用户行为进行分析，而原始数据中只有一行一行的用户发生时刻数据。通过写SQL又太繁琐，我们也许可以尝试一下mapreduce！

程序大体分为三个部分：
1.驱动程序：用于初始化、提交mapreduce job。
2.mapper、reducer程序：具体的计算逻辑。
3.异常处理、工具等。

也许看一张简单的程序流程图能更好的理解这个计算过程：
![image](https://github.com/hupujrs2017/MapReduceUserStayTime/blob/master/src/main/resources/userstaytime.png)
执行该mapreduce的脚本：
hadoop jar MapReduceUserStayTime-1.0-SNAPSHOT-with-dependencies.jar com.yangyz.ubt.UserStayTime 2017-08-20 09-18-24 ;
