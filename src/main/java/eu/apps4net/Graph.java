package eu.apps4net;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
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
    private static final String TMP_PATH = "output_tmp";    // Τοποθεσία για την πρώτη (προσωρινή) εκτέλεση του προγράμματος
    private final static int minimumIn = 3; // Ελάχιστος αριθμός εισερχόμενων ακμών
    private final static int minimumOut = 2;    // Ελάχιστος αριθμός εξερχόμενων ακμών

    public static class GraphMapper extends Mapper<Object, Text, Text, IntWritable> {
        private final Text node = new Text();
        private final IntWritable inOut = new IntWritable();

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            Pair pair;

            // Ανάγνωση του ζεύγους κορυφών
            try {
                pair = new Pair(line);
            } catch (IllegalArgumentException e) {
                System.out.println(e.getMessage());
                return;
            }

            // Για κάθε κορυφή του ζευγαριού θέτει αν έχει εισερχόμενη ή εξερχόμενη ακμή

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

    // O reducer εξάγει το αποτέλεσμα στη μορφή "κορυφή (εξερχόμενες, εισερχόμενες)"
    public static class GraphInOutReducer extends Reducer<Text, IntWritable, Text, Text> {
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

            // Αν οι εισερχόμενες/εξερχόμενες ακμές δεν είναι αρκετές τότε δεν εμφανίζουμε την κορυφή
            if(out < minimumOut || in < minimumIn) {
                return;
            }

            // Εμφανίζουμε το αποτέλεσμα
            result.set(String.format("(%d, %d)", out, in));

            context.write(key, result);
        }
    }

    // O reducer εξάγει το αποτέλεσμα στη μορφή "κορυφή βαθμός"
    public static class GraphDegreeReducer extends Reducer<Text, IntWritable, Text, Text> {
        private final Text result = new Text();

        public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            Configuration conf = context.getConfiguration();
            double avg = 0;

            // Αν έχει οριστεί μέσος όρος των βαθμών των κορυφών τότε τον πέρνουμε στην μεταβλητή avg
            if(conf.get("AVG") != null) {
                avg = Double.parseDouble(conf.get("AVG"));
            }

            int sum = 0;

            // Αθροίζουμε τις εξερχόμενες και εισερχόμενες ακμές
            for (IntWritable val : values) {
                sum ++;
            }

            // Αν ο βαθμός είναι μικρότερος του μέσου όρου των βαθμών των κορυφών τότε δεν εμφανίζουμε την κορυφή
            if(avg > 0 && sum < avg) {
                return;
            }

            // Εμφανίζουμε το αποτέλεσμα
            result.set(String.format("%d", sum));

            context.write(key, result);
        }
    }

    /**
     * Υπολογίζει των μέσο όρο των βαθμών των κορυφών
     *
     * @return double
     */
    private static double calculateAVG() throws IOException {
        int degreeSum = 0;
        int counter = 0;

        // Διαβάζει το αρχείο αποτελεσμάτων
        File fs = new File(TMP_PATH + "/part-r-00000");
        if (!fs.exists()) {
            throw new IOException("Output not found!");
        }

        BufferedReader br = new BufferedReader(new FileReader(fs));
        String line;

        // Αθροίζει τους βαθμούς και μετράει τις κορυφές
        while ((line = br.readLine()) != null) {
            // Μετατρέπει τη γραμμή σε tokens (node, degree)
            StringTokenizer st = new StringTokenizer(line);

            String node = st.nextToken();   // Η κορυφή δε μας ενδιαφέρει και την προσπερνούμε
            String degree = st.nextToken(); // Ο βαθμός της κορυφής

            degreeSum += Integer.parseInt(degree);
            counter++;
        }

        // Υπολογίζει και επιστρέφει το μέσο όρο
        return (double) degreeSum / counter;
    }

    /**
     * Διαγράφει τον προσωρινό φάκελο
     *
     * @param directoryToBeDeleted String
     */
    private static void deleteDirectory(File directoryToBeDeleted) {
        File[] allContents = directoryToBeDeleted.listFiles();

        if (allContents != null) {
            for (File file : allContents) {
                deleteDirectory(file);
            }
        }

        directoryToBeDeleted.delete();
    }

    public static void main(String[] args) throws Exception {
        Class<? extends Reducer> reducerClass = null;
        String jobName = "";

        // Έλεγχος αν έχει δοθεί παράμετρος για επιλογή του προγράμματος που θα τρέξει
        if(args[2] == null) {
            System.out.println("Add 3rd parameter to specify the program you want to run (1 or 2)");
            return;
        }

        // 1: Για να τρέξει το Α που ζητάει η εργασία. 2: για να τρέξει το Β
        // Θέτει τον αντίστοιχο reducer και το όνομα του job
        if(args[2].equals("1")) {
            reducerClass = GraphInOutReducer.class;
            jobName = "Graph In and Out Nodes";
        } else {
            reducerClass = GraphDegreeReducer.class;
            jobName = "Graph nodes degree";
        }

        // Πρώτη εκτέλεση MapReduce
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, jobName);
        job.setJarByClass(Graph.class);
        job.setMapperClass(GraphMapper.class);
        job.setReducerClass(reducerClass);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        // To output path είναι το TMP_PATH αν η τρίτη παράμετρος είναι 2
        FileOutputFormat.setOutputPath(job, new Path(args[2].equals("1") ? args[1] : TMP_PATH));

        // Αν η τρίτη παράμετρος είναι 1 τότε το πρόγραμμα τερματίζει μετά το πρώτο job
        if(args[2].equals("1")) {
            System.exit(job.waitForCompletion(true) ? 0 : 1);
        }

        // Περιμένει να τελειώσει το πρώτο job
        job.waitForCompletion(true);

        // Υπολογισμός μέσου όρου των βαθμών των κορυφών
        try {
            double avg = calculateAVG();
            File tmp = new File(TMP_PATH);
            deleteDirectory(tmp);

            // Αποθηκεύει τον μέσο όρο στο configuration
            conf.setDouble("AVG", avg);
        } catch (IOException e) {
            e.printStackTrace();
        }

        // Συνεχίζει ξανατρέχοντας το job, αφού πλέον γνωρίζει το μέσο όρο
        Job jobFinal = Job.getInstance(conf, jobName);
        jobFinal.setJarByClass(Graph.class);
        jobFinal.setMapperClass(GraphMapper.class);
        jobFinal.setReducerClass(reducerClass);
        jobFinal.setOutputKeyClass(Text.class);
        jobFinal.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(jobFinal, new Path(args[0]));
        FileOutputFormat.setOutputPath(jobFinal, new Path(args[1]));
        System.exit(jobFinal.waitForCompletion(true) ? 0 : 1);
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
