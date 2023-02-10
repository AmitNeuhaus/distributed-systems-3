import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
//import org.apache.hadoop.mapred.join.TupleWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileAsTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import java.io.IOException;
import java.util.Iterator;

public class MapperReducer_1 {
    public static class Mapper_1 extends Mapper<LongWritable, Text, TupleWritable, IntWritable> {
        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String sentence = value.toString().split("\t")[1];
            PatternNoun patternNoun = Parser.parse(sentence);
            if(patternNoun != null) {
                TupleWritable n1n2_pattern = new TupleWritable(new Text(patternNoun.noun1 + " " + patternNoun.noun2),  new Text(patternNoun.pattern));
                context.write(n1n2_pattern, new IntWritable(1));
            }
            System.out.println("Finished mapper");
        }
    }


    public static class Reducer_1 extends Reducer<TupleWritable, IntWritable, Text, Text> {
        @Override
        public void reduce(TupleWritable n1n2_pattern, Iterable<IntWritable> amountArray, Context context) throws IOException, InterruptedException {
            Iterator<IntWritable> it = amountArray.iterator();
            IntWritable sum = new IntWritable(0);
            while(it.hasNext()){
                sum = new IntWritable(sum.get() + it.next().get()); // should be 1 for each , but I still add it.next().get()
            }
            context.write(new Text(n1n2_pattern.toString()), new Text(sum.toString()));
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
            job.setOutputKeyClass(TupleWritable.class);
            job.setOutputValueClass(Text.class);
//        job.setInputFormatClass(SequenceFileAsTextInputFormat.class);
            FileInputFormat.addInputPath(job, new Path(args[0]));
            FileOutputFormat.setOutputPath(job, new Path(args[1]));
            System.exit(job.waitForCompletion(true) ? 0 : 1);
        }catch(Exception e){
            System.out.println(e);
        }

    }
}
