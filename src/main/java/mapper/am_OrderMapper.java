package mapper;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import java.io.IOException;

public class am_OrderMapper extends Mapper<LongWritable, Text, Text, Text> {

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String input = value.toString();
        String[] split = input.split("\t");
//         只考虑000001号股票
        if (split[8].equals("000001")) {
//      在这里我们没有限制时间。考虑到在 非 连续竞价时间下的单可能会在连续竞价时间段内撤销、
//      我们仍然需要这些单子，以对应上trade中的撤单数据，最后进行输出。
            // 现在把要最终需要输出的部分提取出来
            // 对于order来说， 我们需要：
            //1. 委托时间（第13位）， 2. 限价单价格（第11位）， 3.委托数量（第12位）， 4.买卖方向（第14位）， 5.委托类别（第15位） 6.委托索引（第8位）
            String price = "";
            String market_type_buy = "_";
            String market_type = "";
            String market_type_sale = "_";

//        判断是否为限价单。
            if (split[14].equals("2")) {
//          只有限价单保留价格，其他单子都为空。
                price = split[10];
            }
//            以下为mapper输出text的格式以及含义
//1. 委托时间（第13位）， 2. 限价单价格（第11位）， 3.委托数量（第12位）， 4.买卖方向（第14位）， 5.委托类别（第15位） 6.委托索引（第8位）
// 7. 市价单价格挡位（自建）， 8.撤单类别（自建， 默认为非撤单）， 9.cancel交易索引（待填入）， 10.买方市价单档位（自建）， 11.卖方市价单档位（自建），
// 12. 价格（自建， 用来区分市价单档位）
            Text result = new Text(split[12] + "," + price + "," + split[11] + "," + split[13] + "," + split[14] + ","
                    + split[7] + "," + market_type+ "," + "2" + "," + "_" + "," + market_type_buy +"," + market_type_sale
                    + "," + " ");

//              key为a_order只是为了在shuffle之后，reducer能先处理order的数据。
            context.write(new Text("a_order"), result);

        }
    }
}
