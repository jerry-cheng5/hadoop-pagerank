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
// import org.apache.hadoop.mapred.jobcontrol.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Counters;
import org.apache.hadoop.mapreduce.lib.jobcontrol.JobControl;
import org.apache.hadoop.mapreduce.lib.jobcontrol.ControlledJob;
// import org.apache.hadoop.mapred.jobcontrol.JobControl;


public class PRPreProcess {

    public static enum NodeCounter {
        COUNT
    }

    public static class TokenizerMapper
            extends Mapper<Object, Text, IntWritable, IntWritable>{

            private final static IntWritable one = new IntWritable(1);

            public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
                String[] s = value.toString().split("\\s+");
                
                if (s.length < 2) { return; }
                
                int curNode = Integer.parseInt(s[0]);
                int destNode = Integer.parseInt(s[1]);

                if (curNode == destNode) {
                    context.write(new IntWritable(curNode), new IntWritable(-1));
                }

                context.write(new IntWritable(curNode), new IntWritable(destNode));
            }
    }

    // (IntWritable id, PRNodeWritable node)
    public static class IntSumReducer
            extends Reducer<IntWritable,IntWritable,IntWritable,PRNodeWritable> {
            // private PRNodeWritable node = new PRNodeWritable();

            public void reduce(IntWritable key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
                PRNodeWritable node = new PRNodeWritable();
                node.setNodeID(key.get());

                List<Integer> aList = new ArrayList<>();
                for (IntWritable i : values) {
                    int id = i.get();
                    if (id >= 0) {
                        aList.add(id);
                    }
                }

                node.setAdjacencyList(aList);
                node.setPRValue(-1.0);

                context.getCounter(NodeCounter.COUNT).increment(1);
                context.write(key, node);
            }
    }

    public static void main(String[] args) throws Exception {

        double alpha = Double.parseDouble(args[0]);
        int iteration = Integer.parseInt(args[1]);
        double threshold = Double.parseDouble(args[2]);

        Configuration conf1 = new Configuration();
        Job job1 = Job.getInstance(conf1, "PR Pre Process");

        FileInputFormat.addInputPath(job1, new Path(args[3]));
        FileOutputFormat.setOutputPath(job1, new Path("intermediate"));

        job1.setJarByClass(PRPreProcess.class);
        job1.setMapperClass(TokenizerMapper.class);
        job1.setReducerClass(IntSumReducer.class);

        job1.setMapOutputValueClass(IntWritable.class);

        job1.setOutputKeyClass(IntWritable.class);
        job1.setOutputValueClass(PRNodeWritable.class);

        job1.waitForCompletion(true);


        Configuration conf2 = new Configuration();
        Job job2 = Job.getInstance(conf2, "PR Page Rank");

        long nodeCount = job1.getCounters().findCounter(NodeCounter.COUNT).getValue();
        job2.getConfiguration().setLong("nodeCount", nodeCount);

        FileInputFormat.addInputPath(job2, new Path("intermediate"));
        FileOutputFormat.setOutputPath(job2, new Path(args[4]));

        job2.setJarByClass(PageRank.class);
        job2.setMapperClass(PageRank.TokenizerMapper2.class);
        job2.setReducerClass(PageRank.IntSumReducer2.class);

        job2.setMapOutputKeyClass(IntWritable.class);
        job2.setMapOutputValueClass(PRNodeWritable.class);

        job2.setOutputKeyClass(IntWritable.class);
        job2.setOutputValueClass(PRNodeWritable.class);

        job2.waitForCompletion(true);

    }
}


    //Archive
        // Configuration conf = new Configuration();
        // Job job1 = Job.getInstance(conf, "PR Pre Process");
        // job1.setJarByClass(PRPreProcess.class);

        // job1.setMapperClass(TokenizerMapper.class);
        // job1.setReducerClass(IntSumReducer.class);

        // job1.setMapOutputValueClass(IntWritable.class);

        // job1.setOutputKeyClass(IntWritable.class);
        // job1.setOutputValueClass(PRNodeWritable.class);

        // FileInputFormat.addInputPath(job1, new Path(args[0]));
        // FileOutputFormat.setOutputPath(job1, new Path("intermediate"));

        // boolean isJob1Successful = job1.waitForCompletion(true);
        // if (isJob1Successful) {
        //     Configuration conf2 = new Configuration();
        //     Job job2 = Job.getInstance(conf2, "PR Page Rank");

        //     long nodeCount = job1.getCounters().findCounter(NodeCounter.COUNT).getValue();
        //     job2.getConfiguration().setLong("nodeCount", nodeCount);

        //     FileInputFormat.addInputPath(job2, new Path("intermediate"));
        //     FileOutputFormat.setOutputPath(job2, new Path(args[1]));

        //     job2.setJarByClass(PRPageRank.class);
        //     job2.setMapperClass(TokenizerMapper.class);
        //     job2.setReducerClass(IntSumReducer.class);
        //     job2.setMapOutputKeyClass(IntWritable.class);
        //     job2.setMapOutputValueClass(PRNodeWritable.class);
        //     job2.setOutputKeyClass(IntWritable.class);
        //     job2.setOutputValueClass(PRNodeWritable.class);

        //     job2.waitForCompletion(true);
        // }


        // Job control code

        // ControlledJob controlledJob1 = new ControlledJob(conf1);
        // controlledJob1.setJob(job1);

        // ControlledJob controlledJob2 = new ControlledJob(conf2);
        // controlledJob2.setJob(job2);

        // controlledJob2.addDependingJob(controlledJob1);

        // JobControl jc = new JobControl("JobControl");
        // jc.addJob(controlledJob1);
        // jc.addJob(controlledJob2);

        // Thread jobControlThread = new Thread(jc);
        // jobControlThread.start();

        // JobControl jc = new JobControl("JobControl");
        // jc.addJob(job1);
        // jc.addJob(job2);
        // job2.addDependingJob(job1);
        // jc.run();