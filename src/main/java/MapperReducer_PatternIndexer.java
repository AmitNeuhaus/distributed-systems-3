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

import java.io.IOException;
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
            }
        }

    }


    public static class PartitionerClass extends Partitioner<Text, Text> {
        @Override
        public int getPartition(Text key, Text value, int numPartitions) {
            return (key.hashCode() & Integer.MAX_VALUE) % numPartitions;
        }
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
            System.exit(job.waitForCompletion(true) ? 0 : 1);
        }catch(Exception e){
            System.out.println(e);
        }

    }
}
