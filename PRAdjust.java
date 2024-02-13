import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.StringTokenizer;

import javax.naming.Context;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Counters;

public class PRAdjust {
    // public static class TokenizerMapper3
    //         extends Mapper<Object, Text, IntWritable, PRNodeWritable>{

    //         private long nodeCount;

    //         protected void setup(Context context) {
    //             Configuration conf = context.getConfiguration();
    //             nodeCount = conf.getLong("nodeCount", 1);
    //         }

    //         public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
    //             PRNodeWritable node = new PRNodeWritable();
    //             node.fromString(value.toString());

    //             // First run
    //             if (node.getPRValue() < (double) 0) {
    //                 node.setPRValue(1 / (double) nodeCount);
    //             }

    //             List<Integer> aList = node.getAdjacencyList();

    //             double p = node.getPRValue() / (double) aList.size();
    //             context.write(new IntWritable(node.getNodeID()), node);

    //             for (int m : aList) {
    //                 context.write(new IntWritable(m), new PRNodeWritable(p));
    //             }
    //         }
    // }

    // public static class IntSumReducer3
    //         extends Reducer<IntWritable,PRNodeWritable,IntWritable,PRNodeWritable> {

    //         public void reduce(IntWritable key, Iterable<PRNodeWritable> values, Context context) throws IOException, InterruptedException {
    //             PRNodeWritable node = new PRNodeWritable();
    //             double newPRValue = 0.0;

    //             for (PRNodeWritable v : values) {
    //                 if (v.getNodeID() != 0) {
    //                     node = new PRNodeWritable(v);
    //                 }
    //                 else {
    //                     newPRValue += v.getPRValue();
    //                 }
    //             }
    //             node.setPRValue(newPRValue);
                
    //             context.write(key, node);
    //         }
    // }

    public static void main(String[] args) throws Exception { }
}
