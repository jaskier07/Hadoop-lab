
import com.google.common.base.Strings;
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

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.ThreadLocalRandom;


public class VisitorStatistics extends Configured implements Tool {

    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new Configuration(), new VisitorStatistics(), args);
        System.exit(res);
    }

    @Override
    public int run(String[] args) throws Exception {
        Configuration conf = this.getConf();
        FileSystem fs = FileSystem.get(conf);
        Path path = new Path("/user/lada14/tmp/");
        fs.delete(path, true);

        // jobA
        Job jobA = Job.getInstance(conf, "Filter out records");

        jobA.setMapOutputKeyClass(IntWritable.class);
        jobA.setMapOutputValueClass(TextArrayWritable.class);

        jobA.setOutputKeyClass(NullWritable.class);
        jobA.setOutputValueClass(Text.class);

        jobA.setMapperClass(ReadLinesMap.class);
        jobA.setReducerClass(NullValuesReduce.class);

        FileInputFormat.setInputPaths(jobA, new Path(args[0]));
        FileOutputFormat.setOutputPath(jobA, path);

        jobA.setJarByClass(VisitorStatistics.class);
        jobA.waitForCompletion(true);

        // jobB
        Job jobB = Job.getInstance(conf, "Visitor statistics");

        jobB.setMapOutputKeyClass(Text.class);
        jobB.setMapOutputValueClass(TextArrayWritable.class);

        jobB.setOutputKeyClass(NullWritable.class);
        jobB.setOutputValueClass(Text.class);

        jobB.setMapperClass(VisitorStatisticsMap.class);
        jobB.setReducerClass(VisitorStatisticsReduce.class);

        FileInputFormat.setInputPaths(jobB, path);
        FileOutputFormat.setOutputPath(jobB, new Path(args[1]));

        jobB.setJarByClass(VisitorStatistics.class);
        return jobB.waitForCompletion(true) ? 0 : 1;
    }

    // ---------------------- part 1 ----------------------

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

    // ------------------------------- part 2 -----------------------------------------

    public static class VisitorStatisticsMap extends Mapper<Object, Text, Text, TextArrayWritable> {

        @Override
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String valueString = value.toString();
            String[] visit = valueString.split(",");

            String visitor = visit[0] + " " + visit[1] + " " + visit[2];
            String date = visit[11];
            String cancelled = (Strings.isNullOrEmpty(visit[13]) ? "false" : "true");

            String[] arrayToWrite = {date, cancelled, Integer.toString(1)};

            context.write(new Text(visitor), new TextArrayWritable(arrayToWrite));
        }
    }

    public static class VisitorStatisticsReduce extends Reducer<Text, TextArrayWritable, NullWritable, Text> {
        private SimpleDateFormat sdf = new SimpleDateFormat("MM/dd/yyyy mm:HH");

        @Override
        public void reduce(Text key, Iterable<TextArrayWritable> values, Context context) throws IOException, InterruptedException {
            String visitor = key.toString();
            Date firstDate = null;
            Date lastDate = null;
            int counter = 0;
            sdf.setLenient(true);

            for (TextArrayWritable textArrayWritable : values) {
                Text[] visitText = (Text[]) textArrayWritable.toArray();
                String[] visit = convertToStringArray(visitText);
                String date = visit[0];
                String cancelled = visit[1];
                String count = visit[2];

                counter += Integer.parseInt(count);

                if (!Boolean.parseBoolean(cancelled)) {
                    Date visitDate = getDate(date);
                    if (visitDate == null) {
                        continue;
                    }

                    if (firstDate != null) {
                        if (visitDate.before(firstDate)) {
                            firstDate = visitDate;
                        } else if (visitDate.after(lastDate)) {
                            lastDate = visitDate;
                        }
                    } else {
                        firstDate = visitDate;
                        lastDate = visitDate;
                    }
                }
            }

            if (firstDate != null) {
                sdf.applyPattern("MM/dd/yyyy mm:HH");
                String textToWrite = visitor + "\t" + sdf.format(firstDate) + "\t" + sdf.format(lastDate) + "\t" + Integer.toString(counter);
                context.write(NullWritable.get(), new Text(textToWrite));
            }
        }

        private String[] convertToStringArray(Text[] textArray) {
            String[] stringArray = new String[textArray.length];
            for (int i = 0; i < textArray.length; i++) {
                stringArray[i] = textArray[i].toString();
            }
            return stringArray;
        }

        private Date getDate(String text) {
            Date date = tryToParseDate(text);
            if (date != null) {
                return date;
            }
            sdf.applyPattern("MM/dd/yyyymm:HH");
            date = tryToParseDate(text);
            if (date != null) {
                return date;
            }
            sdf.applyPattern("MM/dd/yyyy mm:HH");
            return null;
        }

        private Date tryToParseDate(String text) {
            try {
                return sdf.parse(text);
            } catch (Exception e) {
                return null;
            }
        }
    }

    public static class TextArrayWritable extends ArrayWritable {
        public TextArrayWritable() {
            super(Text.class);
        }

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