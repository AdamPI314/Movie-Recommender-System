import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class Normalize {
    public static class NormalizeMapper extends Mapper<LongWritable, Text, Text, Text> {
        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            //movieA:movieB\t relation
            String[] movie_relation = value.toString().trim().split("\t");
            if (movie_relation.length != 2)
                return;
            String movieA = movie_relation[0].split(":")[0];
            String movieB = movie_relation[0].split(":")[1];
            context.write(new Text(movieA), new Text(movieB + "=" + movie_relation[1]));
        }
    }

    public static class NormalizeReducer extends Reducer<Text, Text, Text, Text> {
        @Override
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            // context --> transpose
            int sum_relation = 0;
            Map<String, Integer> map = new HashMap<String, Integer>();
            for (Text value : values) {
                String[] movieB_relation = value.toString().split("=");
                int relation = Integer.parseInt(movieB_relation[1]);
                sum_relation += relation;
                map.put(movieB_relation[0], relation);
            }

            for (Map.Entry<String, Integer> entry : map.entrySet()) {
                String outputKey = entry.getKey();
                String outputValue = key.toString() + "=" + (double) entry.getValue() / sum_relation;
                context.write(new Text(outputKey), new Text(outputValue));
            }
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration configuration = new Configuration();

        Job job = Job.getInstance(configuration);

        job.setMapperClass(NormalizeMapper.class);
        job.setReducerClass(NormalizeReducer.class);

        job.setJarByClass(Normalize.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        job.setOutputValueClass(Text.class);
        job.setOutputValueClass(Text.class);

        TextInputFormat.setInputPaths(job, new Path(args[0]));
        TextOutputFormat.setOutputPath(job, new Path(args[1]));
    }
}
