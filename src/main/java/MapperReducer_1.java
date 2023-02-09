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
    public static class Mapper_1 extends Mapper<LongWritable, Text, TupleWritable, Text> {
        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String sentence = value.toString().split("\t")[1];
            PatternNoun patternNoun = Parser.parse(sentence);
            if(patternNoun != null) {
                TupleWritable n1n2 = new TupleWritable(new Text(patternNoun.noun1), new Text(patternNoun.noun2));
                Text pattern = new Text(patternNoun.pattern);
                context.write(n1n2, pattern);
            }
            System.out.println("Finished mapper");
        }
    }


    public static class Reducer_1 extends Reducer<TupleWritable, Text, TupleWritable, TupleWritable> {
        @Override
        public void reduce(TupleWritable n1n2, Iterable<Text> patterns, Context context) throws IOException, InterruptedException {
            System.out.println("In the reducer");
            Iterator<Text> it = patterns.iterator();
            String currentPattern;
            String previousPattern = "";
            int currentCount = 0;
            if (it.hasNext()) {
                currentPattern = it.next().toString();
                previousPattern = currentPattern;
                currentCount++;
            }
            while (it.hasNext()) {
                currentPattern = it.next().toString();
                if (currentPattern.equals(previousPattern)) {
                    currentCount++;
                } else {
                    context.write(n1n2, getPatternToCountTuple(currentCount, previousPattern));
                    previousPattern = currentPattern;
                    currentCount = 1;
                }
            }
            context.write(n1n2, getPatternToCountTuple(currentCount, previousPattern));
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
            job.setMapOutputValueClass(Text.class);
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
