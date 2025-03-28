package hk.ust.csit5970;

import org.apache.commons.cli.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.*;

/**
 * Compute the bigram count using "stripes" approach
 */
public class CORStripes extends Configured implements Tool {
    private static final Logger LOG = Logger.getLogger(CORStripes.class);

    /*
	 * TODO: write your first-pass Mapper here.
	 */
    private static class CORMapper1 extends Mapper<LongWritable, Text, Text, IntWritable> {
        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            HashSet<String> wordsInLine = new HashSet<>();
            // Please use this tokenizer! DO NOT implement a tokenizer by yourself!
			String clean_doc = value.toString().replaceAll("[^a-z A-Z]", " ");
            StringTokenizer doc_tokenizer = new StringTokenizer(clean_doc);
            
            while (doc_tokenizer.hasMoreTokens()) {
                String word = doc_tokenizer.nextToken();
                if (!word.isEmpty()) {
                    wordsInLine.add(word);
                }
            }

            for (String word : wordsInLine) {
                context.write(new Text(word), new IntWritable(1));
            }
        }
    }

    /*
	 * TODO: Write your first-pass reducer here.
	 */
    private static class CORReducer1 extends Reducer<Text, IntWritable, Text, IntWritable> {
        @Override
        public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int totalCount = 0;
            for (IntWritable val : values) {
                totalCount += val.get();
            }
            context.write(key, new IntWritable(totalCount)); // Emit the total frequency of the word
        }
    }

    /*
	 * TODO: Write your second-pass Mapper here.
	 */
    public static class CORStripesMapper2 extends Mapper<LongWritable, Text, Text, MapWritable> {
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            Set<String> sorted_word_set = new TreeSet<>();
            // Please use this tokenizer! DO NOT implement a tokenizer by yourself!
            String doc_clean = value.toString().replaceAll("[^a-z A-Z]", " ");
            StringTokenizer doc_tokenizers = new StringTokenizer(doc_clean);
            while (doc_tokenizers.hasMoreTokens()) {
                sorted_word_set.add(doc_tokenizers.nextToken());
            }

            MapWritable wordPairCountMap = new MapWritable();
            List<String> wordList = new ArrayList<>(sorted_word_set);
            for (int i = 0; i < wordList.size(); i++) {
                for (int j = i + 1; j < wordList.size(); j++) {
                    String word1 = wordList.get(i);
                    String word2 = wordList.get(j);
                    String pair = word1.compareTo(word2) < 0 ? word1 + " " + word2 : word2 + " " + word1;

                    if (wordPairCountMap.containsKey(new Text(pair))) {
                        IntWritable count = (IntWritable) wordPairCountMap.get(new Text(pair));
                        count.set(count.get() + 1);
                    } else {
                        wordPairCountMap.put(new Text(pair), new IntWritable(1));
                    }
                }
            }
            context.write(new Text("word_pair"), wordPairCountMap);
        }
    }

    /*
	 * TODO: Write your second-pass Combiner here.
	 */
    public static class CORStripesCombiner2 extends Reducer<Text, MapWritable, Text, MapWritable> {
        @Override
        protected void reduce(Text key, Iterable<MapWritable> values, Context context) throws IOException, InterruptedException {
            
            MapWritable combinedMap = new MapWritable();


            for (MapWritable map : values) {
                for (Map.Entry<Writable, Writable> entry : map.entrySet()) {
                    Text pair = (Text) entry.getKey();
                    IntWritable count = (IntWritable) entry.getValue();

              
                    if (combinedMap.containsKey(pair)) {
                        IntWritable existingCount = (IntWritable) combinedMap.get(pair);
                        existingCount.set(existingCount.get() + count.get());
                    } else {
                        combinedMap.put(pair, new IntWritable(count.get()));
                    }
                }
            }
            context.write(key, combinedMap); 
        }
    }

    /*
	 * TODO: Write your second-pass Reducer here.
	 */
    public static class CORStripesReducer2 extends Reducer<Text, MapWritable, PairOfStrings, DoubleWritable> {
        private static Map<String, Integer> word_total_map = new HashMap<>();

        /*
		 * Preload the middle result file.
		 * In the middle result file, each line contains a word and its frequency Freq(A), seperated by "\t"
		 */
        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            Path middle_result_path = new Path("mid/part-r-00000");
            FileSystem fs = FileSystem.get(context.getConfiguration());

            if (!fs.exists(middle_result_path)) {
                throw new IOException(middle_result_path.toString() + " not exist!");
            }

            BufferedReader reader = new BufferedReader(new InputStreamReader(fs.open(middle_result_path)));
            
            LOG.info("reading...");
            String line;
            while ((line = reader.readLine()) != null) {
                String[] parts = line.split("\t");
                word_total_map.put(parts[0], Integer.parseInt(parts[1]));
                LOG.info("read one line!");
            }
            reader.close();
            LOG.info("finished！");
        }

        /*
		 * TODO: Write your second-pass Reducer here.
		 */
        @Override
        protected void reduce(Text key, Iterable<MapWritable> values, Context context) throws IOException, InterruptedException {
            MapWritable combinedMap = new MapWritable();
            for (MapWritable map : values) {
                for (Map.Entry<Writable, Writable> entry : map.entrySet()) {
                    Text pair = (Text) entry.getKey();
                    IntWritable count = (IntWritable) entry.getValue();

                    if (combinedMap.containsKey(pair)) {
                        IntWritable existingCount = (IntWritable) combinedMap.get(pair);
                        existingCount.set(existingCount.get() + count.get());
                    } else {
                        combinedMap.put(pair, new IntWritable(count.get()));
                    }
                }
            }

            for (Map.Entry<Writable, Writable> entry : combinedMap.entrySet()) {
                Text pair = (Text) entry.getKey();
                IntWritable freqAB = (IntWritable) entry.getValue();
                String[] words = pair.toString().split(" ");
                String word1 = words[0];
                String word2 = words[1];

                int freqWord1 = word_total_map.containsKey(word1) ? word_total_map.get(word1) : 0;
                int freqWord2 = word_total_map.containsKey(word2) ? word_total_map.get(word2) : 0;  

                if (freqWord1 > 0 && freqWord2 > 0) {
                    double correlation = (double) freqAB.get() / (freqWord1 * freqWord2);
                    context.write(new PairOfStrings(word1, word2), new DoubleWritable(correlation));
                }
            }
        }
    }

    /**
     * Creates an instance of this tool.
     */
    public CORStripes() {}

    private static final String INPUT = "input";
    private static final String OUTPUT = "output";
    private static final String NUM_REDUCERS = "numReducers";

    /**
     * Runs this tool.
     */
    @SuppressWarnings({ "static-access" })
    public int run(String[] args) throws Exception {
        // Use fully qualified class name for Options
        org.apache.commons.cli.Options options = new org.apache.commons.cli.Options();

        options.addOption(OptionBuilder.withArgName("path").hasArg()
                .withDescription("input path").create(INPUT));
        options.addOption(OptionBuilder.withArgName("path").hasArg()
                .withDescription("output path").create(OUTPUT));
        options.addOption(OptionBuilder.withArgName("num").hasArg()
                .withDescription("number of reducers").create(NUM_REDUCERS));

        CommandLine cmdline;
        CommandLineParser parser = new GnuParser(); // Changed to GnuParser

        try {
            cmdline = parser.parse(options, args);
        } catch (ParseException exp) {
            System.err.println("Error parsing command line: " + exp.getMessage());
            return -1;
        }

        // Lack of arguments
        if (!cmdline.hasOption(INPUT) || !cmdline.hasOption(OUTPUT)) {
            System.out.println("args: " + Arrays.toString(args));
            HelpFormatter formatter = new HelpFormatter();
            formatter.setWidth(120);
            formatter.printHelp(this.getClass().getName(), options);
            ToolRunner.printGenericCommandUsage(System.out);
            return -1;
        }

        String inputPath = cmdline.getOptionValue(INPUT);
        String middlePath = "mid";
        String outputPath = cmdline.getOptionValue(OUTPUT);

        int reduceTasks = cmdline.hasOption(NUM_REDUCERS) ? Integer
                .parseInt(cmdline.getOptionValue(NUM_REDUCERS)) : 1;

        LOG.info("Tool: " + CORStripes.class.getSimpleName());
        LOG.info(" - input path: " + inputPath);
        LOG.info(" - middle path: " + middlePath);
        LOG.info(" - output path: " + outputPath);
        LOG.info(" - number of reducers: " + reduceTasks);

        // Setup for the first-pass MapReduce
        Configuration conf1 = new Configuration();

        Job job1 = Job.getInstance(conf1, "Firstpass");

        job1.setJarByClass(CORStripes.class);
        job1.setMapperClass(CORMapper1.class);
        job1.setReducerClass(CORReducer1.class);
        job1.setOutputKeyClass(Text.class);
        job1.setOutputValueClass(IntWritable.class);

        FileInputFormat.setInputPaths(job1, new Path(inputPath));
        FileOutputFormat.setOutputPath(job1, new Path(middlePath));

        // Time the program
        long startTime = System.currentTimeMillis();
        job1.waitForCompletion(true);
        LOG.info("Job 1 Finished in " + (System.currentTimeMillis() - startTime)
                / 1000.0 + " seconds");

        // Setup for the second-pass MapReduce

        Configuration conf2 = new Configuration();
        Job job2 = Job.getInstance(conf2, "Secondpass");

        job2.setJarByClass(CORStripes.class);
        job2.setMapperClass(CORStripesMapper2.class);
        job2.setCombinerClass(CORStripesCombiner2.class);
        job2.setReducerClass(CORStripesReducer2.class);

        job2.setOutputKeyClass(PairOfStrings.class);
        job2.setOutputValueClass(DoubleWritable.class);
        job2.setMapOutputKeyClass(Text.class);
        job2.setMapOutputValueClass(MapWritable.class);
        job2.setNumReduceTasks(reduceTasks);

        FileInputFormat.setInputPaths(job2, new Path(inputPath));
        FileOutputFormat.setOutputPath(job2, new Path(outputPath));

        // Time the program
        startTime = System.currentTimeMillis();
        job2.waitForCompletion(true);
        LOG.info("Job 2 Finished in " + (System.currentTimeMillis() - startTime)
                / 1000.0 + " seconds");

        return 0;
    }

    /**
     * Dispatches command-line arguments to the tool via the {@code ToolRunner}.
     */
    public static void main(String[] args) throws Exception {
        ToolRunner.run(new CORStripes(), args);
    }
}
