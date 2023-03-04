import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.AutoCloseableLock;

import java.io.IOException;
import java.util.Iterator;
import java.util.StringJoiner;
import java.util.concurrent.locks.Lock;


public class MapperReducer_DPMIN {

    public static Integer DPMIN = 2;
    public static class Mapper_DPMIN extends Mapper<LongWritable, Text, Text, TupleWritable> {
        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            PatternNoun patternNoun = Parser.getPatternNoun(value.toString().split("\t")[0]);
            context.write(new Text(patternNoun.pattern), new TupleWritable(new Text(patternNoun.noun1), new Text(patternNoun.noun2)));
        }
    }

    public static class Reducer_DPMIN extends Reducer<Text, TupleWritable, Text, Text> {
        @Override
        public void reduce(Text pattern, Iterable<TupleWritable> n1n2, Context context) throws IOException, InterruptedException {
            Iterator<TupleWritable> it = n1n2.iterator();
            Integer amountOfNounsPerPattern = 0;
            while (it.hasNext()) {
                TupleWritable nextPatternAmount = it.next();
                amountOfNounsPerPattern = amountOfNounsPerPattern+1;
            }
            if (amountOfNounsPerPattern >= DPMIN) {
//                try (AutoCloseableLock lock = new AutoCloseableLock(context)) {
//                    context.getCounter(PatternCountEnum.PATTERN_COUNT).increment(1);
//                    Long counter = context.getCounter(PatternCountEnum.PATTERN_COUNT).getValue();
                    context.write(pattern, new Text(amountOfNounsPerPattern.toString()));
                }
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
            job.setJarByClass(MapperReducer_DPMIN.class);
            job.setMapperClass(Mapper_DPMIN.class);
            job.setPartitionerClass(PartitionerClass.class);
            job.setReducerClass(Reducer_DPMIN.class);
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
            }


        }catch(Exception e){
            System.out.println(e);
        }

    }
}
