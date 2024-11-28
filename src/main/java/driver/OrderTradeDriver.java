package driver;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import mapper.*;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.commons.lang3.time.StopWatch;
import reducer.*;
import java.io.IOException;


public class OrderTradeDriver {
    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
        Configuration conf = new Configuration();
        StopWatch stopWatch = new StopWatch();
        stopWatch.start();

        // 设置Reduce处理逻辑及输出类型
        Job job_time = Job.getInstance(conf, "TimeFilter");
        job_time.setJarByClass(OrderTradeDriver.class);

        // 早上逐笔委托数据处理
        MultipleInputs.addInputPath(job_time, new Path("data/am_hq_order_spot.txt"),
                TextInputFormat.class, am_OrderMapper.class);
        // 下午逐笔委托数据处理
        MultipleInputs.addInputPath(job_time, new Path("data/pm_hq_order_spot.txt"),
                TextInputFormat.class, pm_OrderMapper.class);
//        上午逐笔成交数据处理
        MultipleInputs.addInputPath(job_time, new Path("data/am_hq_trade_spot.txt"),
                TextInputFormat.class, am_TradeMapper.class);
//        下午逐笔成交数据处理
        MultipleInputs.addInputPath(job_time, new Path("data/pm_hq_trade_spot.txt"),
                TextInputFormat.class, pm_TrederMapper.class);

//     在docker上跑需要的输入代码--------------------------------------------------------------------------
//        // 早上逐笔委托数据处理
//        MultipleInputs.addInputPath(job_time, new Path(args[0]),
//                TextInputFormat.class, am_OrderMapper.class);
//        // 下午逐笔委托数据处理
//        MultipleInputs.addInputPath(job_time, new Path(args[1]),
//                TextInputFormat.class, pm_OrderMapper.class);
////        上午逐笔成交数据处理
//        MultipleInputs.addInputPath(job_time, new Path(args[2]),
//                TextInputFormat.class, am_TradeMapper.class);
////        下午逐笔成交数据处理
//        MultipleInputs.addInputPath(job_time, new Path(args[3]),
//                TextInputFormat.class, pm_TrederMapper.class);
//-------------------------------------------------------------------------------------------------------------

        job_time.setReducerClass(TimeReducer.class);
        job_time.setOutputKeyClass(Text.class);
        job_time.setOutputValueClass(Text.class);
        job_time.getConfiguration().set("mapreduce.output.basename", "output");

//         设置输出路径
        TextOutputFormat.setOutputPath(job_time, new Path("output/final_4.8" +
                ""));

//  在docker上跑需要的输出代码
//        FileOutputFormat.setOutputPath(job_time, new Path(args[4]));

        if(job_time.waitForCompletion(true)){
            stopWatch.stop();
            float time = (float)stopWatch.getTime()/1000;
            System.out.println("耗时：" + time + "s");
        }
        System.exit(job_time.waitForCompletion(true) ? 0 : 1);
    }

}


