package edu.umich.cse.eecs485;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
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

public class InvertedIndex4 {
    public static class Map extends Mapper<LongWritable, Text, Text, Text> {
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException { 
      
        String line = value.toString();
        String[] strs1 = line.split("\\s+");
        String docid = strs1[0];
        String[] triples = strs1[1].split(",");
        String[] triple_vals;
        
        String term, df, tfidf;
        for (int i = 0; i < triples.length; i ++) {
            triple_vals = triples[i].split(":"); 
            term = triple_vals[0];
            df = triple_vals[1];
            tfidf = triple_vals[2];
            context.write(new Text(String.format("%s:%s", term, df)), new Text(String.format("%s:%s", docid, tfidf)) );
      }
    }
  }

  public static class Reduce extends Reducer<Text, Text, Text, Text> {
    public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
      
        // splitting 
        String[] term_df;
        String[] docid_tfidf;
        String docid, tfidf;

        term_df = key.toString().split(":");
        StringBuilder sb = new StringBuilder();
        for (Text value : values) {
            docid_tfidf = value.toString().split(":");
            docid = docid_tfidf[0]; 
            tfidf = docid_tfidf[1]; 

            sb.append(String.format("%s:%s ", docid, tfidf));
        }
        
        // Reformat as the requirements
        sb.deleteCharAt(sb.length()-1);
        sb.insert(0, String.format("%s ", term_df[1]));
       
        context.write(new Text(term_df[0]), new Text(sb.toString()));
    }
  }

  public static void main(String[] args) throws Exception
  {
      Configuration conf = new Configuration();

      Job job = new Job(conf, "InvertedIndex4");

      job.setOutputKeyClass(Text.class);
      job.setOutputValueClass(Text.class);

      job.setMapperClass(Map.class);
      job.setReducerClass(Reduce.class);

      job.setInputFormatClass(TextInputFormat.class);
      job.setOutputFormatClass(TextOutputFormat.class);

      FileInputFormat.addInputPath(job, new Path(args[0]));
      FileOutputFormat.setOutputPath(job, new Path(args[1]));

      job.waitForCompletion(true);
  }
}
