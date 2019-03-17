package lab1;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
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
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;
import java.lang.Integer;
import java.util.StringTokenizer;
import java.util.TreeSet;

// >>> Don't Change
public class TopPopularLinks extends Configured implements Tool {
    public static final Log LOG = LogFactory.getLog(TopPopularLinks.class);

    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new Configuration(), new TopPopularLinks(), args);
        System.exit(res);
    }

    public static class IntArrayWritable extends ArrayWritable {
        public IntArrayWritable() {
            super(IntWritable.class);
        }

        public IntArrayWritable(Integer[] numbers) {
            super(IntWritable.class);
            IntWritable[] ints = new IntWritable[numbers.length];
            for (int i = 0; i < numbers.length; i++) {
                ints[i] = new IntWritable(numbers[i]);
            }
            set(ints);
        }
    }
// <<< Don't Change

    @Override
    public int run(String[] args) throws Exception {
        Configuration conf = this.getConf();
        Job job = Job.getInstance(conf, "Orphan pages");
        FileSystem fs = FileSystem.get(conf);
        Path path = new Path("/user/lada14/lab1-2/tmp/");
        fs.delete(path, true);

        // REDUCE
        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(IntWritable.class);
        // MAP
        job.setMapOutputKeyClass(IntWritable.class);
        job.setMapOutputValueClass(IntWritable.class);

        job.setMapperClass(LinkCountMap.class);
        job.setReducerClass(LinkCountReduce.class);

        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, path);

        job.setJarByClass(TopPopularLinks.class);
        job.waitForCompletion(true);

        Job job2 = Job.getInstance(conf, "Orphan pages 2");

        job2.setOutputKeyClass(IntWritable.class);
        job2.setOutputValueClass(IntWritable.class);

        job2.setMapOutputKeyClass(NullWritable.class);
        job2.setMapOutputValueClass(IntArrayWritable.class);

        job2.setMapperClass(TopLinksMap.class);
        job2.setReducerClass(TopLinksReduce.class);

        FileInputFormat.setInputPaths(job2, path);
        FileOutputFormat.setOutputPath(job2, new Path(args[1]));

        job2.setInputFormatClass(KeyValueTextInputFormat.class);
        job2.setOutputFormatClass(TextOutputFormat.class);
        job2.setNumReduceTasks(1);

        job2.setJarByClass(TopPopularLinks.class);

        return job2.waitForCompletion(true) ? 0 : 1;
    }

    public static class LinkCountMap extends Mapper<Object, Text, IntWritable, IntWritable> {
        @Override
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            StringTokenizer tokenizer = new StringTokenizer(line, ": ");
            if (tokenizer.hasMoreTokens()) {
                Integer from = new Integer(tokenizer.nextToken());
                while (tokenizer.hasMoreTokens()) {
                    Integer to = new Integer(tokenizer.nextToken());
                    context.write(new IntWritable(to), new IntWritable(1));
                }
                //	context.write(new IntWritable(from), new IntWritable(0));
            }
        }
    }

    public static class LinkCountReduce extends Reducer<IntWritable, IntWritable, IntWritable, IntWritable> {
        @Override
        public void reduce(IntWritable key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable val : values) {
                sum += val.get();
            }
            context.write(new IntWritable(new Integer(key.get())), new IntWritable(sum));
        }
    }
    public static class TopLinksMap extends Mapper<Text, Text, NullWritable, IntArrayWritable> {
        Integer N;
        TreeSet<Pair3<Integer, Integer>> set = new TreeSet<>();

        @Override
        protected void setup(Context context) throws IOException,InterruptedException {
            Configuration conf = context.getConfiguration();
            this.N = conf.getInt("N", 10);
        }

        @Override
        public void map(Text key, Text value, Context context) throws IOException, InterruptedException {
            Integer key2 = new Integer(key.toString());
            Integer count = new Integer(value.toString());
            set.add(new Pair3<Integer, Integer>(count, key2));
        }


        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            for (int i = 0; i < N; i++) {
                Pair3<Integer, Integer> last = set.pollLast();
                if (last != null) {
                    Integer[] intArray = { last.first, last.second };
                    IntArrayWritable array = new IntArrayWritable(intArray);
                    context.write(NullWritable.get(), array);
                }
            }
        }
    }

    public static class TopLinksReduce extends Reducer<NullWritable, IntArrayWritable, IntWritable, IntWritable> {
        Integer N;

        @Override
        protected void setup(Context context) throws IOException,InterruptedException {
            Configuration conf = context.getConfiguration();
            this.N = conf.getInt("N", 10);
        }

        @Override
        public void reduce(NullWritable key, Iterable<IntArrayWritable> values, Context context) throws IOException, InterruptedException {
            for (IntArrayWritable array : values) {
                IntWritable[] intArray = (IntWritable[])array.toArray();
                context.write(intArray[0], intArray[1]);
            }
        }
    }
}

// >>> Don't Change
class Pair3<A extends Comparable<? super A>,
        B extends Comparable<? super B>>
        implements Comparable<Pair3<A, B>> {

    public final A first;
    public final B second;

    public Pair3(A first, B second) {
        this.first = first;
        this.second = second;
    }

    public static <A extends Comparable<? super A>,
            B extends Comparable<? super B>>
    Pair3<A, B> of(A first, B second) {
        return new Pair3<A, B>(first, second);
    }

    @Override
    public int compareTo(Pair3<A, B> o) {
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
        if (!(obj instanceof Pair3))
            return false;
        if (this == obj)
            return true;
        return equal(first, ((Pair3<?, ?>) obj).first)
                && equal(second, ((Pair3<?, ?>) obj).second);
    }

    private boolean equal(Object o1, Object o2) {
        return o1 == o2 || (o1 != null && o1.equals(o2));
    }

    @Override
    public String toString() {
        return "(" + first + ", " + second + ')';
    }
}
// <<< Don't Change
