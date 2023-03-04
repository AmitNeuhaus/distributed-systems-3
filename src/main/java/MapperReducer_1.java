import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
//import org.apache.hadoop.mapred.join.TupleWritable;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileAsTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import java.io.IOException;
import java.util.Iterator;

public class MapperReducer_1 {
    public static class Mapper_1 extends Mapper<LongWritable, Text, TupleWritable, IntWritable> {
        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

            try {
                String sentence = value.toString().split("\t")[1];
                PatternNoun patternNoun = Parser.parse(sentence);
                if (patternNoun != null) {
                    TupleWritable n1n2_pattern = new TupleWritable(new Text(patternNoun.noun1 + " " + patternNoun.noun2), new Text(patternNoun.pattern));
                    context.write(n1n2_pattern, new IntWritable(1));
                }
            }catch (Exception e){
                System.out.println(e);
            }
        }
    }


    public static class Reducer_1 extends Reducer<TupleWritable, IntWritable, Text, Text> {
        @Override
        public void reduce(TupleWritable n1n2_pattern, Iterable<IntWritable> amountArray, Context context) throws IOException, InterruptedException {
            try{
                Iterator<IntWritable> it = amountArray.iterator();
                IntWritable sum = new IntWritable(0);
                while(it.hasNext()){
                sum = new IntWritable(sum.get() + it.next().get()); // should be 1 for each , but I still add it.next().get()
            }
                context.write(new Text(n1n2_pattern.toString()), new Text(sum.toString()));
            }catch (Exception e){
                System.out.println(e);
            }
        }
    }

    private static TupleWritable getPatternToCountTuple(int currentCount, String previousPattern) {
        return new TupleWritable(new Text(previousPattern), new Text(Integer.toString(currentCount)));
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
            Job job = Job.getInstance(conf, "Step_1");
            job.setJarByClass(MapperReducer_1.class);
            job.setMapperClass(Mapper_1.class);
            job.setPartitionerClass(PartitionerClass.class);
            job.setReducerClass(Reducer_1.class);
            job.setMapOutputKeyClass(TupleWritable.class);
            job.setMapOutputValueClass(IntWritable.class);
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(Text.class);
//        job.setInputFormatClass(SequenceFileAsTextInputFormat.class);
            FileInputFormat.addInputPath(job, new Path(args[0]));
            FileOutputFormat.setOutputPath(job, new Path(args[1]));

//            conf.set("hadoop.log.dir", "/Desktop/log/hadoop/syslog");
//            conf.set("hadoop.log.file", "hadoop.log");
//            conf.set("hadoop.root.logger", "INFO, syslog");
//            conf.set("hadoop.log.syslog.facility", "LOCAL4");
//            conf.set("hadoop.log.syslog.level", "INFO");
//            conf.set("hadoop.log.syslog.layout", "org.apache.log4j.PatternLayout");
//            conf.set("hadoop.log.syslog.layout.ConversionPattern", "%d{ISO8601} %-5p %c{2} (%F:%M(%L)) - %m%n");


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

