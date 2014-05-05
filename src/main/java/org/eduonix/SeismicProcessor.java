package org.eduonix;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;
import java.math.BigDecimal;

/**
 * Created by ubu on 5/4/14.
 */
public class SeismicProcessor {

    static boolean isAmazon = true;
    private static final String BASE_URL = "s3n://eduonix";
    private static Path dataInEMR = new Path(BASE_URL+"/input" ,"seismic");
    private static Path dataOutEMR  = new Path( BASE_URL+"/result/seismicProcessor");
    private static Path localInput = new Path("./testData","seismic");
    private static Path localOutput = new Path("./seismicDataOut");

    static class PreprocessorMapper extends Mapper<LongWritable, Text, Text, Text> {

        private Text wordKey = new Text();
        private Text wordValue = new Text();

        @Override
        protected void setup(Context context) throws IOException,
                InterruptedException {
            super.setup(context);
        }

        @Override
        protected void map(LongWritable key, Text value, Context context)  throws java.io.IOException, InterruptedException {

            String line = value.toString();
            String[] tokens = line.split(" ");
            if (tokens == null || tokens.length != 2) {
                System.err.print("Passing header line with itext line: " + line + "n");
                return;
            }
            calculateValue( tokens);
            context.write(wordKey, wordValue);

        }



        private void calculateValue(String[] tokens) {

            BigDecimal key = BigDecimal.valueOf(Math.abs(Double.valueOf(tokens[0])));
            BigDecimal value = BigDecimal.valueOf(Double.valueOf(tokens[1]));
            key = key.movePointRight(6);
            value = value.movePointRight(6);
            wordKey.set(String.valueOf(key.intValue()));
            wordValue.set(String.valueOf(value.intValue()));
        }

    }

    public static class PostProcessorReducer  extends Reducer<Text, Text, Text, Text> {

        public void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {

            String lKey = key.toString();
            String lVal =  values.iterator().next().toString();
            String aggregatedKey = lKey+"\t"+lVal;

            BigDecimal tempKey = BigDecimal.valueOf(Math.abs(Double.valueOf(lKey)));
            BigDecimal tempValue = BigDecimal.valueOf(Double.valueOf(lVal));
            BigDecimal signalValue = tempKey.multiply(tempValue);
            Text  signalValueTxt = new Text(signalValue.toPlainString());
            Text  aggregatedKeyTxt = new Text(aggregatedKey);

            context.write(aggregatedKeyTxt, signalValueTxt);
        }
    }



    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {

        Configuration conf = new Configuration();



        Job job = new Job(conf, "seismicProcessor");

        job.setReducerClass(PostProcessorReducer.class);
        job.setMapperClass(PreprocessorMapper.class);
        job.setJarByClass(SeismicProcessor.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        if(isAmazon){
            FileInputFormat.addInputPath(job, dataInEMR);
            FileOutputFormat.setOutputPath(job, dataOutEMR);


        } else {
            FileSystem.get(localOutput.toUri(), conf).delete(localOutput, true);
            FileInputFormat.addInputPath(job, localInput);
            FileOutputFormat.setOutputPath(job, localOutput);
        }

        job.submit();
        System.out.println(job);
        job.waitForCompletion(true);
    }






}
