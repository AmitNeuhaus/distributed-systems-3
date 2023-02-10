import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;

import java.io.IOException;
import java.util.*;


public class MapperReducer_Join {

    public static class Mapper_Join_Noun_Pattern extends Mapper<LongWritable, Text, Text, TupleWritable> {
        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            PatternNoun patternNoun = Parser.getPatternNoun(value.toString().split("\t")[0]);
            String count = value.toString().split("\t")[1];
            context.write(new Text(patternNoun.pattern), new TupleWritable(new Text(patternNoun.noun1 + " " + patternNoun.noun2 +"-"+ count),new Text("noun")));
        }
    }

    public static class Mapper_Join_Pattern_Index extends Mapper<LongWritable, Text, Text, TupleWritable> {
        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String pattern = value.toString().split("\t")[0];
            String index = value.toString().split("\t")[1];
            context.write(new Text(pattern), new TupleWritable(new Text(index), new Text("index")));
        }
    }

    public static class Reducer_JOIN extends Reducer<Text, TupleWritable, Text, Text> {
        @Override
        public void reduce(Text pattern, Iterable<TupleWritable> n1n2, Context context) throws IOException, InterruptedException {

            // classify values to 2 groups (pattern  noun count ("noun"), pattern index ("index))

            Text recordOfTypeIndex = new Text("index");
            Text recordOfTypeNoun = new Text("noun");

            HashMap<Text, List<TupleWritable>> table = new HashMap();
            table.put(new Text("index"), new ArrayList<>());
            table.put(new Text("noun"), new ArrayList<>());


            Iterator<TupleWritable> it = n1n2.iterator();
            while (it.hasNext()) {
                TupleWritable record = it.next();
                Text recordType = record.getSecond();
                if (recordType.equals(recordOfTypeNoun)) {
                    String nouns = record.getFirst().toString().split("-")[0];
                    String count = record.getFirst().toString().split("-")[1];
                    TupleWritable formattedRecord = new TupleWritable(new Text(nouns), new Text(count));
                    List<TupleWritable> listOfNounCounts = table.get(recordType);
                    listOfNounCounts.add(formattedRecord);
                    table.put(recordType, listOfNounCounts);
                } else if (recordType.equals(recordOfTypeIndex)) {
                    Text patternIndex = record.getFirst();
                    TupleWritable formattedRecord = new TupleWritable(pattern, patternIndex);
                    List<TupleWritable> listOfPatternIndex = table.get(recordType); // applying list even if we have only 1
                    listOfPatternIndex.add(formattedRecord);
                    table.put(recordType, listOfPatternIndex);
                } else {
                    System.out.println("Couldn't indicate record type");
                }
            }
                if(table.get(recordOfTypeIndex).size() == 0){
                    System.out.println(pattern + "didnt have index record, therefore it didnt pass the DPMIN");
                }else {
                    Text patternIndex = table.get(recordOfTypeIndex).get(0).getSecond();
                    //creating context cross product:
                    for (int i = 0; i < table.get(recordOfTypeNoun).size(); i++) {
                        Text nouns = table.get(recordOfTypeNoun).get(i).getFirst();
                        Text count = table.get(recordOfTypeNoun).get(i).getSecond();
                        context.write(patternIndex, new Text(nouns.toString() + "-" + count.toString()));
                    }
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
            try {
                Configuration conf = new Configuration();
                Job job = Job.getInstance(conf, "Step_Nouns");
                job.setJarByClass(MapperReducer_Join.class);
                MultipleInputs.addInputPath(job, new Path(args[0]), TextInputFormat.class, Mapper_Join_Noun_Pattern.class);
                MultipleInputs.addInputPath(job, new Path(args[1]), TextInputFormat.class, Mapper_Join_Pattern_Index.class);
                job.setReducerClass(MapperReducer_Join.Reducer_JOIN.class);
                job.setMapOutputKeyClass(Text.class);
                job.setMapOutputValueClass(TupleWritable.class);
                job.setPartitionerClass(PartitionerClass.class);
                job.setOutputKeyClass(Text.class);
                job.setOutputValueClass(Text.class);
                FileOutputFormat.setOutputPath(job, new Path(args[2]));
                System.exit(job.waitForCompletion(true) ? 0 : 1);
            } catch (Exception e) {
                System.out.println(e);
            }

        }




}


