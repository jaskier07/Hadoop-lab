

import com.google.common.base.Strings;
import org.apache.commons.lang.math.NumberUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.omg.CORBA.ExceptionList;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Random;
import java.util.Set;
import java.util.StringTokenizer;
import java.util.concurrent.ThreadLocalRandom;


/**


    hadoop jar Percentage.jar Percentage -D percentage=PERCENTAGE /user/adampap/WHVsmall
 */
public class Percentage extends Configured implements Tool {

    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new Configuration(), new Percentage(), args);
        System.exit(res);
    }

    @Override
    public int run(String[] args) throws Exception {
        Job jobA = Job.getInstance(this.getConf(), "Filter out records");

        jobA.setMapOutputKeyClass(IntWritable.class);
        jobA.setMapOutputValueClass(TextArrayWritable.class);

        jobA.setOutputKeyClass(NullWritable.class);
        jobA.setOutputValueClass(Text.class);

        jobA.setMapperClass(ReadLinesMap.class);
        jobA.setReducerClass(NullValuesReduce.class);

        FileInputFormat.setInputPaths(jobA, new Path(args[0]));
        FileOutputFormat.setOutputPath(jobA, new Path(args[1]));

        jobA.setJarByClass(Percentage.class);

        return jobA.waitForCompletion(true) ? 0 : 1;
    }

    public static class ReadLinesMap extends Mapper<Object, Text, IntWritable, TextArrayWritable> {

        @Override
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String valueString = value.toString();
            String[] visit = valueString.split(",");
            TextArrayWritable t = new TextArrayWritable(visit);
            context.write(new IntWritable(1), t);
        }
    }

    public static class NullValuesReduce extends Reducer<IntWritable, TextArrayWritable, NullWritable, Text> {
        private List<String[]> visits = new ArrayList<>();
        int percentage;

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            Configuration conf = context.getConfiguration();
            percentage = conf.getInt("percentage", 10);
            if (percentage < 0 || percentage > 100) {
                throw new IllegalArgumentException("Provide number in range <0;100>");
            }
        }

        @Override
        public void reduce(IntWritable key, Iterable<TextArrayWritable> values, Context context) throws IOException, InterruptedException {
            for (TextArrayWritable array : values) {
                String[] visit = array.toStrings();
                if (!Strings.isNullOrEmpty(visit[6]) && !Strings.isNullOrEmpty(visit[11])) {
                    visits.add(visit);
                }
            }
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            Set<Integer> drawnIndexes = new HashSet<>();
            int iterations = (visits.size() * percentage) / 100;

            while (iterations-- > 0) {
                Integer index = drawNumber(drawnIndexes);
                String str = Arrays.toString(visits.get(index)).replace(" ", "");
                str = str.substring(1, str.length() - 1);
                context.write(NullWritable.get(), new Text(str));
            }
        }

        private Integer drawNumber(Set<Integer> drawnIndexes) {
            while (true) {
                Integer index = ThreadLocalRandom.current().nextInt(0, visits.size() - 1);
                if (!drawnIndexes.contains(index)) {
                    drawnIndexes.add(index);
                    return index;
                }
            }
        }
    }
    public static class TextArrayWritable extends ArrayWritable {
        public TextArrayWritable(String[] strings) {
            super(Text.class);
            Text[] texts = new Text[strings.length];
            for (int i = 0; i < strings.length; i++) {
                texts[i] = new Text(strings[i]);
            }
            set(texts);
        }
    }
}