import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
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
            System.exit(job.waitForCompletion(true) ? 0 : 1);
        }catch(Exception e){
            System.out.println(e);
        }

    }
}
