import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.*;

public class MapperReducer_Nouns {
    public static class Mapper_Nouns extends Mapper<LongWritable, Text, Text, TupleWritable> {
        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String patternIndex = value.toString().split("\t")[0];
            String nouns = value.toString().split("\t")[1].split("-")[0];
            String count = value.toString().split("\t")[1].split("-")[1];


            context.write(new Text(nouns), new TupleWritable(new Text(patternIndex), new Text(count)));
        }

    }

    public static class Reducer_Nouns extends Reducer<Text, TupleWritable, Text, Text> {
        @Override
        public void reduce(Text n1n2, Iterable<TupleWritable> patternAmount, Context context) throws IOException, InterruptedException {
            Iterator<TupleWritable> it = patternAmount.iterator();
            StringJoiner patterns = new StringJoiner(",");
            while (it.hasNext()) {
                TupleWritable nextPatternAmount = it.next();
                Text patternIndex = nextPatternAmount.getFirst();
                Text count = nextPatternAmount.getSecond();
                patterns.add(patternIndex.toString() + '-' + count.toString());
            }
            context.write(n1n2, new Text(patterns.toString()));
        }
    }


    public static class PartitionerClass extends Partitioner<TupleWritable, Text> {
        @Override
        public int getPartition(TupleWritable key, Text value, int numPartitions) {
            return (key.hashCode() & Integer.MAX_VALUE) % numPartitions;
        }
    }

    public static void main(String[] args) throws Exception {
        try{
            Configuration conf = new Configuration();
            Job job = Job.getInstance(conf, "Step_Nouns");
            job.setJarByClass(MapperReducer_Nouns.class);
            job.setMapperClass(Mapper_Nouns.class);
            job.setPartitionerClass(PartitionerClass.class);
            job.setReducerClass(Reducer_Nouns.class);
            job.setMapOutputKeyClass(Text.class);
            job.setMapOutputValueClass(TupleWritable.class);
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(Text.class);
//        job.setInputFormatClass(SequenceFileAsTextInputFormat.class);
            FileInputFormat.addInputPath(job, new Path(args[0]));
            FileOutputFormat.setOutputPath(job, new Path(args[1]));
            boolean jobSuccessful = job.waitForCompletion(true);
            if (jobSuccessful) {
                // get the counters for the job
                Counters counters = job.getCounters();

                // output the counters
                // get the counter names for reducer input key-value pairs
                long reducerInputRecords = counters.findCounter("org.apache.hadoop.mapreduce.TaskCounter", "REDUCE_INPUT_RECORDS").getValue();
                System.out.println("Reducer input key-value pairs: " + reducerInputRecords);

                // get the counter names for mapper input key-value pairs
                long mapperInputRecords = counters.findCounter("org.apache.hadoop.mapreduce.TaskCounter", "MAP_INPUT_RECORDS").getValue();
                System.out.println("Mapper input key-value pairs: " + mapperInputRecords);

                // get the counter names for mapper output key-value pairs
                long mapperOutputRecords = counters.findCounter("org.apache.hadoop.mapreduce.TaskCounter", "MAP_OUTPUT_RECORDS").getValue();
                System.out.println("Mapper output key-value pairs: " + mapperOutputRecords);


                Counter outputKeysCounter = counters.findCounter("OutputKeys", "NumKeys");
                System.out.println("Reducer output " + outputKeysCounter.getValue() + " keys.");
                // get the counter name for the total bytes processed by the job
                // get the counter name for the total bytes written by the job
                FileSystem fs = FileSystem.get(conf);
                long totalBytes = counters.findCounter(FileOutputFormat.class.getName(), "BYTES_WRITTEN").getValue();
                System.out.println("Total bytes written by the job: " + totalBytes);
                // add more counters as needed
                long physicalMemoryBytes = counters.findCounter("org.apache.hadoop.mapred.Task$Counter", "PHYSICAL_MEMORY_BYTES").getValue();
                long virtualMemoryBytes = counters.findCounter("org.apache.hadoop.mapred.Task$Counter", "VIRTUAL_MEMORY_BYTES").getValue();
                long committedHeapBytes = counters.findCounter("org.apache.hadoop.mapred.Task$Counter", "COMMITTED_HEAP_BYTES").getValue();

                // print the memory-related counters
                System.out.println("Physical memory bytes: " + physicalMemoryBytes);
                System.out.println("Virtual memory bytes: " + virtualMemoryBytes);
                System.out.println("Committed heap bytes: " + committedHeapBytes);
            }           }catch(Exception e){
            System.out.println(e);
        }

    }
}
