
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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.ThreadLocalRandom;


public class TopVisitors extends Configured implements Tool {

    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new Configuration(), new TopVisitors(), args);
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

        jobA.setJarByClass(TopVisitors.class);
        jobA.waitForCompletion(true);

        // jobB
        Job jobB = Job.getInstance(conf, "Top 10 visitor-visitee");

        jobB.setMapOutputKeyClass(Text.class);
        jobB.setMapOutputValueClass(IntWritable.class);

        jobB.setOutputKeyClass(Text.class);
        jobB.setOutputValueClass(IntWritable.class);

        jobB.setMapperClass(VisitorVisiteeMap.class);
        jobB.setReducerClass(VisitorVisiteeReduce.class);

        FileInputFormat.setInputPaths(jobB, path);
        FileOutputFormat.setOutputPath(jobB, new Path(args[1]));

        jobB.setJarByClass(TopVisitors.class);
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
    
    public static class VisitorVisiteeMap extends Mapper<Object, Text, Text, IntWritable> {

        @Override
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String valueString = value.toString();
            String[] visit = valueString.split(",");
            String visitor = visit[0] + " " + visit[1] + " " + visit[2];
            String visitee = visit[19] + " " + visit[20];
            String textToWrite = visitor + " <------- " + visitee;
            context.write(new Text(textToWrite), new IntWritable(1));
        }
    }

    public static class VisitorVisiteeReduce extends Reducer<Text, IntWritable, Text, IntWritable> {
        private TreeSet<Pair<Integer, Text>> visitorVisitees = new TreeSet<>();
        
        @Override
        public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {

            int sum = 0;
            for (IntWritable i : values) {
                sum += i.get();
            }

            visitorVisitees.add(new Pair<>(sum, key));
            
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            for (int i = 0; i < 10; i++) {
                Pair<Integer, Text> pair = visitorVisitees.pollLast();
                if (pair != null) {
                    Text textToWrite = pair.second;
                    context.write(textToWrite, new IntWritable(pair.first));
                }
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



class Pair<A extends Comparable<? super A>,
        B extends Comparable<? super B>>
        implements Comparable<Pair<A, B>> {

    public final A first;
    public final B second;

    public Pair(A first, B second) {
        this.first = first;
        this.second = second;
    }

    public static <A extends Comparable<? super A>,
            B extends Comparable<? super B>>
    Pair<A, B> of(A first, B second) {
        return new Pair<A, B>(first, second);
    }

    @Override
    public int compareTo(Pair<A, B> o) {
        int cmp = o == null ? 1 : (this.first).compareTo(o.first);
        return cmp == 0 ? (this.second).compareTo(o.second) : cmp;
    }

    @Override
    public int hashCode() {
        return 31 * hashcode(first) + hashcode(second);
    }

    private static int hashcode(Object o) {
        return o == null ? 0 : o.hashCode();
    }

    @Override
    public boolean equals(Object obj) {
        if (!(obj instanceof Pair))
            return false;
        if (this == obj)
            return true;
        return equal(first, ((Pair<?, ?>) obj).first)
                && equal(second, ((Pair<?, ?>) obj).second);
    }

    private boolean equal(Object o1, Object o2) {
        return o1 == o2 || (o1 != null && o1.equals(o2));
    }

    @Override
    public String toString() {
        return "(" + first + ", " + second + ')';
    }
}