import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.util.Iterator;


public class MapperReducer_PatternIndexer {
    public static class Mapper_PatternIndexer extends Mapper<LongWritable, Text, Text, Text> {
        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String pattern = value.toString().split("\t")[0];
            context.write(new Text("P"), new Text(pattern));
        }
    }

    public static class Reducer_PatternIndexer extends Reducer<Text, Text, Text, LongWritable> {
        @Override
        public void reduce(Text P, Iterable<Text> patterns, Context context) throws IOException, InterruptedException {
            Long count = 0L;
            Iterator<Text> it = patterns.iterator();
            while (it.hasNext()) {
                Text pattern = it.next();
                context.write(pattern, new LongWritable(count));
                count = count + 1;
                Counter counter = context.getCounter("pattern-counter-group", "pattern-counter");
                counter.increment(1);
            }
        }

    }


    public static class PartitionerClass extends Partitioner<Text, Text> {
        @Override
        public int getPartition(Text key, Text value, int numPartitions) {
            return (key.hashCode() & Integer.MAX_VALUE) % numPartitions;
        }
    }



    private static void writeNumberOfPatterns(Configuration conf, Job job) throws IOException {
        Path path = new Path("patterns-amount.txt");
        FileSystem fs = FileSystem.get(conf);
        OutputStream os = fs.create(path);
        Writer writer = new OutputStreamWriter(os);
        Counter counter = job.getCounters().findCounter("pattern-counter-group", "pattern-counter");
        writer.write(counter.getValue() + "\n");
        writer.close();
    }
    public static void main(String[] args) throws Exception {
        try{
            Configuration conf = new Configuration();
            Job job = Job.getInstance(conf, "Step_Nouns");
            job.setJarByClass(MapperReducer_PatternIndexer.class);
            job.setMapperClass(Mapper_PatternIndexer.class);
            job.setPartitionerClass(PartitionerClass.class);
            job.setReducerClass(Reducer_PatternIndexer.class);
            job.setMapOutputKeyClass(Text.class);
            job.setMapOutputValueClass(Text.class);
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(LongWritable.class);
//        job.setInputFormatClass(SequenceFileAsTextInputFormat.class);
            FileInputFormat.addInputPath(job, new Path(args[0]));
            FileOutputFormat.setOutputPath(job, new Path(args[1]));

            boolean success = job.waitForCompletion(true);

            if (success) {
                long counterValue = job.getCounters().findCounter("pattern-counter-group", "pattern-counter").getValue();
                writeNumberOfPatterns(conf,job);
                System.out.println("Counter value: " + counterValue);

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

            System.exit(success ? 0 : 1);
        }catch(Exception e){
            System.out.println(e);
        }

    }
}
