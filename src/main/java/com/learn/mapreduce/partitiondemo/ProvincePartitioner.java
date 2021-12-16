package com.learn.mapreduce.partitiondemo;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

/**
 * Description:
 * date: 2021/12/16 15:18
 * Package: com.learn.mapreduce.partitiondemo
 *
 * @author 李佳乐
 * @email 18066550996@163.com
 */
@SuppressWarnings("all")
public class ProvincePartitioner extends Partitioner<Text, FlowBean> {

    private static final String PARTITION_1 = "136";

    private static final String PARTITION_2 = "137";

    private static final String PARTITION_3 = "138";

    private static final String PARTITION_4 = "139";

    @Override
    public int getPartition(Text text, FlowBean flowBean, int numPartitions) {
        String phone = text.toString();
        String prePhone = phone.substring(0, 3);
        int partition;
        if (prePhone.equals(PARTITION_1)) {
            partition = 0;
        } else if (prePhone.equals(PARTITION_2)) {
            partition = 1;
        } else if (prePhone.equals(PARTITION_3)) {
            partition = 2;
        } else if (prePhone.equals(PARTITION_4)) {
            partition = 3;
        } else {
            partition = 4;
        }
        return partition;
    }
}
