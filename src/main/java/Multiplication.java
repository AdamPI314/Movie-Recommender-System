import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.chain.ChainMapper;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class Multiplication {
    public static class CoOccurrenceMapper extends Mapper<LongWritable, Text, Text, Text> {
        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            //movieB \t movieA=relation
            String[] line = value.toString().trim().split("\t");
            context.write(new Text(line[0]), new Text(line[1]));
        }
    }

    public static class RatingMapper extends Mapper<LongWritable, Text, Text, Text> {
        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            // user,movie,rating
            String[] line = value.toString().trim().split(",");
            context.write(new Text(line[1]), new Text(line[0] + ":" + line[2]));
        }
    }

    public static class MultiplicationReducer extends Reducer<Text, Text, Text, DoubleWritable> {
        @Override
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            //key = movieB
            //value = <movieA=relation, movieC=relation, userA:rating, userB:rating...>
            Map<String, Double> relationMap = new HashMap<String, Double>();
            Map<String, Double> ratingMap = new HashMap<String, Double>();

            for (Text value : values) {
                if (value.toString().contains("=")) {
                    String[] movie_relation = value.toString().trim().split("=");
                    relationMap.put(movie_relation[0], Double.parseDouble(movie_relation[1]));
                } else if (value.toString().contains(":")) {
                    String[] user_rating = value.toString().trim().split(":");
                    ratingMap.put(user_rating[0], Double.parseDouble(user_rating[1]));
                }
            }

            for (Map.Entry<String, Double> entry1 : relationMap.entrySet()) {
                String movieA = entry1.getKey();
                Double relation = entry1.getValue();

                for (Map.Entry<String, Double> entry2 : ratingMap.entrySet()) {
                    String user = entry2.getKey();
                    Double rating = entry2.getValue();

                    String outputKey = user + ":" + movieA;
                    double outputValue = relation * rating;
                    context.write(new Text(outputKey), new DoubleWritable(outputValue));
                }
            }

        }
    }

    public static void main(String[] args) throws Exception {
        Configuration configuration = new Configuration();

        Job job = Job.getInstance(configuration);
        job.setJarByClass(Multiplication.class);

        ChainMapper.addMapper(job, CoOccurrenceMapper.class, LongWritable.class, Text.class, Text.class, Text.class, configuration);
        ChainMapper.addMapper(job, RatingMapper.class, Text.class, Text.class, Text.class, Text.class, configuration);

        job.setMapperClass(CoOccurrenceMapper.class);
        job.setMapperClass(RatingMapper.class);

        job.setReducerClass(MultiplicationReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(DoubleWritable.class);

        MultipleInputs.addInputPath(job, new Path(args[0]), TextInputFormat.class, CoOccurrenceMapper.class);
        MultipleInputs.addInputPath(job, new Path(args[1]), TextInputFormat.class, RatingMapper.class);

        TextOutputFormat.setOutputPath(job, new Path(args[2]));

        job.waitForCompletion(true);

    }
}
