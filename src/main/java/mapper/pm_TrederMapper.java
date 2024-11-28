package mapper;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import java.io.IOException;

public class pm_TrederMapper extends Mapper<LongWritable, Text, Text, Text> {

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String input = value.toString();
        String[] split = input.split("\t");
        // 第16位是委托时间
        // 一个时间的字段是这样表示的 20190102091500010
        // 事实上我们只用考虑后面9位 即 091500010
        long transact_time = Long.parseLong(split[15].substring(8));
        //  接下来我们把需要的部分提取出来
        // 对于trade来说，我们需要：
        // 1.成交时间（仅对于撤销的订单， 第16位）， 2.成交类别（第15位）， 3.买方委托索引（第11位）， 4.卖方委托索引（第12位） 5. 证券代码(第9位)
        String trade_time = "";
        String exec_type = split[14];
        String buy_index = split[10];
        String sale_index = split[11];
        String SecurityID = split[8];
        String price = split[12];
//        这是对交易索引进行补位。 在同一个时间下的撤单数据要在非撤单数据之后。
        String trade_seq = "9999999" + split[7];
        //考虑00001 证券代码
        if(SecurityID.equals("000001")){

//   以下为mapper输出text的格式以及含义
// 1.成交时间（仅对于撤销的订单， 第16位）， 2.成交类别（第15位）， 3.买方委托索引（第11位）， 4.卖方委托索引（第12位） 5.市价单档位（待填写）
// 6.成交索引， 7.成交单价格（第13位）

            if (transact_time <= 145700000 && transact_time >= 130000000) {
                // 午
                if (exec_type.equals("4")) {
                    //撤销单， 时间为交易时间
                    trade_time = split[15];
//              撤单的数据比较特殊，在reducer中只需简单处理并可以直接等待排序。
//              所以我们将撤单的数据单独给了一个 key，这样方便reducer中的处理
                    Text result = new Text(trade_time + "," + exec_type + "," + buy_index + "," + sale_index + ","
                            + " " + "," + trade_seq + "," + price);
                    context.write(new Text("cancel"), result);
                } else {
//              只有市价单需要多次处理
//              其他类型的单子都可以直接等待排序。
//              但我们并不知道任何一个交易单的类型，所以只能将非撤单的数据全部输出。
                    Text result = new Text(trade_time + "," + exec_type + "," + buy_index + "," + sale_index + ","
                            + " " + "," + trade_seq + "," + price);
                    context.write(new Text("trade"), result);
                }
            }
        }
    }
}
