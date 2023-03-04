import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import java.io.IOException;
import java.util.Iterator;
import java.util.StringTokenizer;

public class MapperReducer_Labels {
    public static class Mapper_Join_Vector extends Mapper<LongWritable, Text, Text, Text> {
        @Override
        public  void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] nounsToVector = value.toString().split("\t");
            String nouns = nounsToVector[0];
            String n1 = nouns.split(" ")[0].trim();
            String n2 = nouns.split(" ")[1].trim();
            context.write(new Text(n1 + " " + n2), new Text(nounsToVector[1]));
        }
    }

    public static class Mapper_Join_Label extends Mapper<LongWritable, Text, Text, Text>{
        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            StringTokenizer tokenizer = new StringTokenizer(value.toString());;
            String n1, n2, label;
            n1 = tokenizer.hasMoreTokens() ? (String)tokenizer.nextElement() : null;
            n2 = tokenizer.hasMoreTokens() ? (String)tokenizer.nextElement() : null;
            label = tokenizer.hasMoreTokens() ? (String)tokenizer.nextElement() : null;
            if (n1 != null && n2 != null && label != null){
                context.write(new Text(n1 + " " + n2), new Text(label));
            }
        }
    }

    public static class Reducer_Join_Vector_Labels extends Reducer<Text, Text,Text, Text> {
        @Override
        public void reduce(Text n1n2, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            Iterator<Text> it = values.iterator();
            String label = "";
            String vector = "";
            while(it.hasNext()){
                Text current = it.next();
                if (current.toString().equals("True") ||current.toString().equals("False")){
                    label = current.toString();
                }else{
                    vector = current.toString();
                }
            }
            if (!label.equals("") && !vector.equals("")){
                Text vectorAndLabel = new Text(vector + ":" + label);
                context.write(n1n2, vectorAndLabel);
            }
        }
    }
    public static class PartitionerClass extends Partitioner<Text, Text> {
        @Override
        public int getPartition(Text key, Text value, int numPartitions) {
            return (key.hashCode() & Integer.MAX_VALUE) % numPartitions;
        }
    }

    public static void main(String[] args) {
        try {
            Configuration conf = new Configuration();
            Job job = Job.getInstance(conf, "Step_Produce_Labeled_Data");
            job.setJarByClass(MapperReducer_Labels.class);
            MultipleInputs.addInputPath(job, new Path(args[0]), TextInputFormat.class, Mapper_Join_Vector.class);
            MultipleInputs.addInputPath(job, new Path(args[1]), TextInputFormat.class, Mapper_Join_Label.class);
            job.setReducerClass(Reducer_Join_Vector_Labels.class);
            job.setMapOutputKeyClass(Text.class);
            job.setMapOutputValueClass(Text.class);
            job.setPartitionerClass(MapperReducer_Labels.PartitionerClass.class);
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(Text.class);
            FileOutputFormat.setOutputPath(job, new Path(args[2]));
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
            }         } catch (Exception e) {
            System.out.println(e);
        }

    }
}
