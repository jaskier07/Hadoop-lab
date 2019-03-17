package lab1;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.*;
import org.apache.hadoop.io.LongWritable;

public class PopularityLeague extends Configured implements Tool {
    public static final Log LOG = LogFactory.getLog(PopularityLeague.class);

    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new Configuration(), new PopularityLeague(), args);
        System.exit(res);
    }

    @Override
    public int run(String[] args) throws Exception {
        Configuration conf = this.getConf();
        FileSystem fs = FileSystem.get(conf);
        Path path = new Path("/user/lada14/tmp/");
        fs.delete(path, true);

        Job jobA = Job.getInstance(conf, "Popularity league");

        jobA.setMapOutputKeyClass(IntWritable.class);
        jobA.setMapOutputValueClass(IntWritable.class);

        jobA.setOutputKeyClass(IntWritable.class);
        jobA.setOutputValueClass(IntWritable.class);

        jobA.setMapperClass(LinkCountMap.class);
        jobA.setReducerClass(LinkCountReduce.class);

        FileInputFormat.setInputPaths(jobA, new Path(args[0]));
        FileOutputFormat.setOutputPath(jobA, path);

        jobA.setJarByClass(PopularityLeague.class);
        jobA.waitForCompletion(true);

        Job jobB = Job.getInstance(conf, "Pop Leag 2");

        jobB.setMapOutputKeyClass(IntWritable.class);
        jobB.setMapOutputValueClass(IntWritable.class);

        jobB.setOutputKeyClass(IntWritable.class);
        jobB.setOutputValueClass(IntWritable.class);

        jobB.setMapperClass(LeagueMap.class);
        jobB.setReducerClass(LeagueReduce.class);

        FileInputFormat.setInputPaths(jobB, path);
        FileOutputFormat.setOutputPath(jobB, new Path(args[1]));

        jobB.setJarByClass(PopularityLeague.class);
        return jobB.waitForCompletion(true) ? 0 : 1;
    }

    public static class LinkCountMap extends Mapper<Object, Text, IntWritable, IntWritable> {

        List<Integer> leagues = new ArrayList<>();

        @Override
        protected void setup(Context context) throws IOException,InterruptedException {

            Configuration conf = context.getConfiguration();

            String leaguePath = conf.get("league");

            List<String> leaguesString = Arrays.asList(readHDFSFile(leaguePath, conf).split("\n"));
            for (String l : leaguesString) {
                leagues.add(new Integer(l));
            }
        }

        @Override
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            StringTokenizer tokenizer = new StringTokenizer(line, ": ");
            if (tokenizer.hasMoreTokens()) {
                Integer from = new Integer(tokenizer.nextToken());
                while (tokenizer.hasMoreTokens()) {
                    Integer to = new Integer(tokenizer.nextToken());
                    if (leagues.contains(to)) {
                        context.write(new IntWritable(to), new IntWritable(1));
                    }
                }
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
            context.write(key, new IntWritable(sum));
        }
    }

    public static class LeagueMap extends Mapper<LongWritable, Text, IntWritable, IntWritable> {
        List<Integer> leagues = new ArrayList<>();
        List<Pair1<Integer, Integer>> popularity = new ArrayList<>();

        @Override
        protected void setup(Mapper.Context context) throws IOException,InterruptedException {

            Configuration conf = context.getConfiguration();

            String leaguePath = conf.get("league");

            List<String> leaguesString = Arrays.asList(readHDFSFile(leaguePath, conf).split("\n"));
            for (String l : leaguesString) {
                leagues.add(new Integer(l));
            }
        }


        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String s = value.toString();
            String[] array = s.split("[^0-9]");

            popularity.add(new Pair1<>(new Integer(array[0]), new Integer(array[1])));

        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            for (int i = 0; i < popularity.size(); i++) {
                Pair1<Integer, Integer> pair1 = popularity.get(i);
                for (int j = i + 1; j < popularity.size(); j++) {
                    Pair1<Integer, Integer> pair2 = popularity.get(j);
                    if (pair1.second > pair2.second) {
                        context.write(new IntWritable(pair1.first), new IntWritable(1));
                        context.write(new IntWritable(pair2.first), new IntWritable(0));
                    } else if (pair1.second < pair2.second) {
                        context.write(new IntWritable(pair2.first), new IntWritable(1));
                        context.write(new IntWritable(pair1.first), new IntWritable(0));
                    } else {
                        context.write(new IntWritable(pair1.first), new IntWritable(0));
                        context.write(new IntWritable(pair2.first), new IntWritable(0));
                    }
                }
            }
        }
    }

    public static class LeagueReduce extends Reducer<IntWritable, IntWritable, IntWritable, IntWritable> {
        @Override
        public void reduce(IntWritable key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable val : values) {
                sum += val.get();
            }
            context.write(key, new IntWritable(sum));
        }
    }

    public static String readHDFSFile(String path, Configuration conf) throws IOException{
        Path pt=new Path(path);
        FileSystem fs = FileSystem.get(pt.toUri(), conf);
        FSDataInputStream file = fs.open(pt);
        BufferedReader buffIn=new BufferedReader(new InputStreamReader(file));

        StringBuilder everything = new StringBuilder();
        String line;
        while( (line = buffIn.readLine()) != null) {
            everything.append(line);
            everything.append("\n");
        }
        return everything.toString();
    }
}


class Pair2<A extends Comparable<? super A>,
        B extends Comparable<? super B>>
        implements Comparable<Pair1<A, B>> {

    public final A first;
    public final B second;

    public Pair2(A first, B second) {
        this.first = first;
        this.second = second;
    }

    public static <A extends Comparable<? super A>,
            B extends Comparable<? super B>>
    Pair1<A, B> of(A first, B second) {
        return new Pair1<A, B>(first, second);
    }

    @Override
    public int compareTo(Pair1<A, B> o) {
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
        if (!(obj instanceof Pair1))
            return false;
        if (this == obj)
            return true;
        return equal(first, ((Pair1<?, ?>) obj).first)
                && equal(second, ((Pair1<?, ?>) obj).second);
    }

    private boolean equal(Object o1, Object o2) {
        return o1 == o2 || (o1 != null && o1.equals(o2));
    }

    @Override
    public String toString() {
        return "(" + first + ", " + second + ')';
    }
}