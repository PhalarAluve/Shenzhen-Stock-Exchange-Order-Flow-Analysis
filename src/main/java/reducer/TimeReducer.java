package reducer;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.TreeMap;

public class TimeReducer extends Reducer<Text, Text, Text, Text> {
    private TreeMap<String,  ArrayList<String[]>> order = new TreeMap<>();

//            为了方便排序， 我们使用了一个treemap， 其中的value也是一个treemap。
//            外层以时间为key， 内层以 委托索引或者交易索引为key， 这样方便实现我们需要的效果.
    private TreeMap<String, TreeMap<String, String[]>> sort = new TreeMap<>();
    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        String indicator = key.toString();
//        通过指示器来分别order表和trade表
        if(indicator.equals("a_order")){
//            而对于treemap数据结构来说，一个key只能对应一个value。
//            所以选择将ArrayList作为value，这样当遇到重复的key值时， value不会丢失。
            for (Text value: values) {
                String[] split = value.toString().split(",");
                if (order.containsKey(split[5])){
//                    检查是否已经存在key值
//                    若存在，直接将对应的ArrayList提取出来，然后再加入一个数组。
                    order.get(split[5]).add(split);
                }
                else {
//                    若不存在，需要新建一个ArrayList并放入数组
                    ArrayList<String[]> tem = new ArrayList<>();
                    tem.add(split);
                    order.put(split[5], tem);
                }
            }
        }
        else if(indicator.equals("cancel")){
//            这里处理撤单的数据
//            需要利用买方和卖方索引来和order这个tree map对应起来
            for (Text value: values) {
                String[] split = value.toString().split(",");
//             提取出买方索引和卖方索引
                String buy_index = split[2];
                String sale_index = split[3];
//                找order中是否有此买方索引，然后看对应数组的买卖方向是否为买（1）
                if(order.containsKey(buy_index)){
//                    这时候返回一个arraylist
//                    要按顺序检查是否符合要求。
//                    找到该 买方索引 所对应的 ArrayList, 这个ArrayList中可能包含多个数组， 我们需要遍历这个arraylist
//                    以找到 交易类型为 买 的数据。
                    for (int i = 0; i < order.get(buy_index).size(); i++) {
                        String[] s = order.get(buy_index).get(i).clone();
                        if(s[3].equals("1")){
//                            找到了对应的订单，将其标记为撤单， 并将订单时间改为交易时间。
                            s[7] = "1";
                            s[0] = split[0];
//                            对于撤销的单子, 我们可以把交易索引放在s[8] 这个位置
//                            在之后排序时需要按照 交易索引 进行排序
//                            在 sorted_add 方法中， 对于撤单的数据，为了简化排序，我们会将交易索引作为key进行排序。
                            s[8] = split[5];
//                          处理好了此String数组， 我们直接把它加到排序数组中
//                          不仅撤单的数据需要加，原先在order中的下单数据也要加进来
                            sorted_add(s,0);
                            sorted_add(order.get(buy_index).get(i), 2);

//                          把两个数组都放到排序树sort中后，为了避免重复处理，还需要将order中的数据删除。
                            order.get(buy_index).remove(i);
                            if(order.get(buy_index).isEmpty()){
//                          如果从arraylist中删去数组后，此arraylist为空
//                                可以直接在order这个treemap中删除此arraylist
                                order.remove(buy_index);
                                break;
                            }

                        }
                    }
                }
                if(order.containsKey(sale_index)){
//              找order中是否有此卖方索引， 然后看对应数组的买卖方向是否为卖（2）
                    for (int i = 0; i < order.get(sale_index).size(); i++) {
                        String[] s = order.get(sale_index).get(i).clone();
//                        同一个索引可能对应多个单子，我们只需要找到交易类型为 卖的单子。
                        if(s[3].equals("2")){
//                            找到了对应的订单，将其标记为撤单， 并将订单时间改为交易时间.
                            s[7] = "1";
                            s[0] = split[0];
//                            对于撤销的单子, 我们可以把交易索引放在s[8] 这个位置
//                            在 sorted_add 方法中， 对于撤单的数据，为了简化排序，我们会将交易索引作为key进行排序。
                            s[8] = split[5];
//                          处理好了此String数组， 我们直接把它加到排序数组中
//                          不仅撤单的数据需要加，原先在order中的下单数据也要加进来
                            sorted_add(s,0);
                            sorted_add(order.get(sale_index).get(i), 2);

//                          把两个数组都放到排序树sort中后，为了避免重复处理，还需要将order中的数据删除。
                            order.get(sale_index).remove(i);
                            if(order.get(sale_index).isEmpty()){
//                          如果从arraylist中删去数组后，此arraylist为空
//                                可以直接在order这个treemap中删除此arraylist
                                order.remove(sale_index);
                                break;
                            }
                        }
                    }
                }
            }
        }
        else{
//            这里处理的是三种订单的数据
//            事实上，我们只需要处理市价单的数据即委托类别：1 的数据
//            在处理非撤单数据之前，我们已经处理的撤单数据，并且在每一次处理完撤单数据后，从order这个treemap中删去了委托单。
//            此时理论上来说， order中应该只有非撤单或者未能在trade中找到对应交易单的委托单
            for (Text value: values){
                String[] split = value.toString().split(",");
//                和cancel相同， 先把买方索引和卖方索引提取出来，方便我们进行查找
                String buy_index = split[2];
                String sale_index = split[3];
                String price = split[6];

//                随后查找买方索引
                if(order.containsKey(buy_index)){
                    ArrayList<String[]> list = order.get(buy_index);
                    int size = list.size();
                    for (int i = 0; i < size; i++) {
                        if ( !list.get(i)[4].equals("1") ){
//                       非市价单的成交单，可以直接放入sort中，不需要更多的操作
                            sorted_add(list.get(i),2);
//                       同撤单相同，在每次sort添加数据之后，都要从order中删去已经处理过的委托单。
//                       这样可以释放内存，避免重复处理。
                            order.get(buy_index).remove(i);
                            size--;
                            if (order.get(buy_index).isEmpty()){
//                          如果动态数组空了，直接将此动态数组删去。
                                order.remove(buy_index);
                                break;
                            }
                        }
                        else if(list.get(i)[7].equals("2") && list.get(i)[3].equals("1")){
//                          这里处理市价单的数据
//                            查找委托单数据是否为买方
//                              这里需要查找price是否相同, 如果相同则不增加市价单的档位
//                              若不相同,就增加市价单的档位
                            if( !list.get(i)[11].equals(price) ){
//                                 这里的思路是想用 第10位(下标为9) 字符串的长度来表示市价单的档位
//                                  所以每当我们找到一个不一样的price的时候,我们就增加这一位的 String 的长度
                                order.get(buy_index).get(i)[9] = order.get(buy_index).get(i)[9] + "_";
                                order.get(buy_index).get(i)[11] = price;
                            }

                        }
                    }
                }
                if(order.containsKey(sale_index)){
                    ArrayList<String[]> list = order.get(sale_index);
                    int size = list.size();
                    for (int i = 0; i < size; i++) {
                        if ( !list.get(i)[4].equals("1") ){
//                       非市价单的成交单，可以直接放入sort中，不需要更多的操作
                            sorted_add(list.get(i),2);
//                       同撤单相同，在每次sort添加数据之后，都要从order中删去已经处理过的委托单。
//                       这样可以释放内存，避免重复处理。
                            order.get(sale_index).remove(i);
                            size--;
                            if (order.get(sale_index).isEmpty()){
//                          如果动态数组空了，直接将此动态数组删去。
                                order.remove(sale_index);
                                break;
                            }
                        }
                        else if(list.get(i)[7].equals("2") && list.get(i)[3].equals("2")){
//                          这里处理市价单的数据
//                           查找委托单数据是否为卖方
//                                这里需要查找price是否相同, 如果相同则不增加市价单的档位
//                                若不相同,就增加市价单的档位
                            if( !list.get(i)[11].equals(price)){
//                                 这里的思路是想用 第11位(下标为10) 字符串的长度来表示市价单的档位
//                                  所以每当我们找到一个不一样的price的时候,我们就增加这一位的String 长度
                                order.get(sale_index).get(i)[10] = order.get(sale_index).get(i)[10] + "_";
                                order.get(sale_index).get(i)[11] = price;
                            }

                        }
                    }
                }
            }
//            现在，order中只会存在两种数据
//            1. 非撤单市价单且能在trade中找到的数据
//            2. 在trade中找不到的数据
//            现在来处理这两种数据
            int len = order.size();
            for (int i = 0; i< len;i++) {
                String k = order.firstKey();
                ArrayList<String[]> arrayList = order.get(k);
                for (int j = 0; j < arrayList.size(); j++){
                    String[] s = arrayList.get(j);
                    if (s[4].equals("1") && s[7].equals("2")) {
//                     该单为市价单，非撤单

                        if(s[3].equals("1"))
//                       该单为买方
                            s[6] = String.valueOf(s[9].length() - 1);
                        else if(s[3].equals("2"))
//                        该单为卖方
                            s[6] = String.valueOf(s[10].length() - 1);
//                        放入sort中， type 为 1.
                        sorted_add(s, 1);
                        arrayList.remove(j);
                    }
//                    如果不是市价单，就说明此order数据在trade中找不到对应的信息。我们仍然将其留下。
                    else {
                        sorted_add(s, 2);
                    }
                }
                order.remove(k);
                if (order.isEmpty()){
                    break;
                }
            }
        }
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
//        最终的输出
        context.write(new Text("TIMESTAMP" + "," + "PRICE" + "," + "SIZE" + "," + "BUY_SELL_FLAG" + "," + "ORDER_TYPE"
                + "," + "ORDER_ID" + "," + "MARKET_ORDER_TYPE" + "," + "CANCEL_TYPE"), null);
//        现在开始排序
//        因为我们使用的嵌套树结构，不用写额外的排序算法，直接遍历这个嵌套树输出就好。
//        此嵌套树升序排序，我们也不要调换它的顺序。
        for (TreeMap<String, String[]> s :sort.values()) {
            for (String[] str:s.values()) {
//              从存储所有信息的String数组中提取出需要的信息，直接输出。
                context.write(new Text(str[0] + "," + str[1] + "," + str[2] + "," + str[3] + "," + str[4]
                        + "," + str[5] + "," + str[6] + "," + str[7]), null);
            }
        }
    }

//    这个方法将处理好的数据放到最终排序的树中
//    撤单的数据，可以直接放进来
//    非撤单的数据，只有市价单要处理，其他都能直接放进来。
//    int type 表示数据类型
//    0 ：撤单
//    1 ：非撤单 市价单
//    2 ：非撤单 其它单
    private void sorted_add (String[] s, int type){
        String time = s[0];
//      只考虑连续竞价时间段内的单子
        long transact_time = Integer.parseInt(time.substring(8));
        if((transact_time >= 93000000 && transact_time <= 113000000)
                || (transact_time <= 145700000 && transact_time >= 130000000)){
//      把时间的格式换为参考30s答案中的格式
            String text = s[0];
            text = text.substring(0,4) + "-" + text.substring(4,6) + "-" + text.substring(6,8) + " " + text.substring(8,10)
                    + ":" + text.substring(10,12) + ":" + text.substring(12,14) + "." + text.substring(14);
            s[0] = text + "000";
//      appseq是 order id, 对于非撤单来说, order id 是在 order.txt中的,
//      对于撤单来说, order id 是在trade中的.
            String appseq = s[5];
//      将价格的格式换为参考30s中的格式
            if(s[1].length() > 4){
                float price = Float.parseFloat(s[1]);
                s[1] = String.valueOf(price);
            }

            if( type==0 ){
//            撤单的数据
//               撤单数据， 用trade中的交易索引排序。
                appseq = s[8];
            }
//            非撤单，可以直接放入。 即 type==2 或者 type==1 可以直接放入sort中。
            if (sort.containsKey(time)) {
//           如果此时间戳已经存在了，就提取出此时间戳对应的树，再在树中放入 key 为 委托索引或交易索引的键值对
                sort.get(time).put(appseq, s);
            } else {
//           如果此时间戳不存在，就新建一个树，将key为 委托索引或交易索引的 键值对放入新的树
//           再将此新的树放入sort这个树
                TreeMap<String, String[]> new_tree = new TreeMap<>();
                new_tree.put(appseq, s);
                sort.put(time, new_tree);
            }
        }
    }
}

