package mapper;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import java.io.IOException;

public class pm_OrderMapper extends Mapper<LongWritable, Text, Text, Text> {
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String input = value.toString();
        String[] split = input.split("\t");
//        只考虑000001号股票
        if(split[8].equals("000001")) {
            // 第13位是委托时间
            // 一个时间的字段是这样表示的 20190102091500010
            // 事实上我们只用考虑后面9位 即 091500010
            long transact_time = Long.parseLong(split[12].substring(8));
            // 现在把要最终需要输出的部分提取出来
            // 对于order来说， 我们需要：
            //1. 委托时间（第13位）， 2. 限价单价格（第11位）， 3.委托数量（第12位）， 4.买卖方向（第14位）， 5.委托类别（第15位） 6.委托索引（第8位）
            String price = "";
            String market_type_buy = "_";
            String market_type = "";
            String market_type_sale = "_";
//          在这里我们考虑了时间为连续竞价时间段结束之前。
//          因为在这个时间段之后的单子，我们不考虑它们是否撤单或这成交。
            if (transact_time <= 145700000) {
                if (split[14].equals("2")) {
                    // 限价单，只有限价单保留价格，其他单子都为空。
                    price = split[10];
                }
//  以下为mapper输出text的格式以及含义
//1. 委托时间（第13位）， 2. 限价单价格（第11位）， 3.委托数量（第12位）， 4.买卖方向（第14位）， 5.委托类别（第15位） 6.委托索引（第8位）
// 7. 市价单价格挡位（自建）， 8.撤单类别（自建， 默认为非撤单）， 9.cancel交易索引（待填入）， 10.买方市价单档位（自建）， 11.卖方市价单档位（自建），
// 12. 价格（自建， 用来区分市价单档位）
                Text result = new Text(split[12] + "," + price + "," + split[11] + "," + split[13] + "," + split[14] + ","
                        + split[7] + "," + market_type+ "," + "2" + "," + split[8] + "," + market_type_buy +"," + market_type_sale
                        + "," + " ");

//              key为a_order只是为了在shuffle之后，reducer能先处理order的数据。
                context.write(new Text("a_order"), result);
            }
        }
    }
}

