package eu.apps4net;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class Graph {
    public static class GraphMapper extends Mapper<Object, Text, Text, IntWritable> {
        private final Text node = new Text();
        private final IntWritable inOut = new IntWritable();
        private int id = 0;

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            Pair pair;

            // Ανάγνωση του ζεύγους κορυφών
            try {
                pair = new Pair(line);
                id++;
            } catch (IllegalArgumentException e) {
                System.out.println(e.getMessage());
                return;
            }

            // Η πρώτη κορυφή έχει εξερχόμενη ακμή
            node.set(String.valueOf(pair.getFirst()));
            inOut.set(1);
            context.write(node, inOut);

            // Η δεύτερη κορυφή έχει εισερχόμενη ακμή
            node.set(String.valueOf(pair.getSecond()));
            inOut.set(-1);
            context.write(node, inOut);
        }
    }

    public static class GraphReducer extends Reducer<Text, IntWritable, Text, Text> {
        private final Text result = new Text();

        public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int in = 0;
            int out = 0;

            // Αθροίζουμε τις εξερχόμενες και εισερχόμενες ακμές
            for (IntWritable val : values) {
               if(val.get() == 1) {
                    out++;
                } else {
                    in++;
                }
            }

            // Εμφανίζουμε το αποτέλεσμα
            result.set(String.format("(%d, %d)", out, in));

            context.write(key, result);
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Graph");
        job.setJarByClass(Graph.class);
        job.setMapperClass(GraphMapper.class);
        job.setReducerClass(GraphReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }

    /**
     * Κλάση για το ζεύγος των κορυφών
     */
    public static class Pair {
        private final int first;
        private final int second;

        public Pair(String line) throws IllegalArgumentException {
            if(!line.matches("^\\d+.*")) {
                throw new IllegalArgumentException("Line does not start with a number");
            }

            String[] words = line.split("\\s+");

            if(words.length != 2) {
                throw new IllegalArgumentException("Line does not contain exactly two words");
            }

            for(String word : words) {
                if(!word.matches("\\d+")) {
                    throw new IllegalArgumentException("Line contains a word that is not a number");
                }
            }

            this.first = Integer.parseInt(words[0]);
            this.second = Integer.parseInt(words[1]);
        }

        public int getFirst() {
            return first;
        }

        public int getSecond() {
            return second;
        }
    }
}
