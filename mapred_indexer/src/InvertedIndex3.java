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
import java.util.*;

public class InvertedIndex3 {
    public static HashSet<String> docsSet = new HashSet<String>();    
    
    public static class Map extends Mapper<LongWritable, Text, Text, Text> {
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException { 
            
            // split into parts
            String line = value.toString();
            String[] str1 = line.split("\\s+");
            String[] str2 = str1[1].split(";");
            String term = str1[0];
            String df = str2[0];
            String list = str2[1];
            
            String[] docs = list.split(",");
            String[] id_tf;
            String docid;
            String tf;
            for (int i=0; i<docs.length; i++) {
                id_tf = docs[i].split(":"); 
                docid = id_tf[0];
                tf = id_tf[1];
                
                // reorganize using docid as the key
                context.write(new Text(docid), new Text(String.format("%s:%s:%s", term, df, tf)));
                docsSet.add(docid);
            }
        }
    }

    public static class Reduce extends Reducer<Text, Text, Text, Text> {
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            String term, df, tf;
            double tfD, dfD, idf, tfidf;
            double normFac = 1.00;
            String[] str;
           
            int totalDocs = docsSet.size();
            ArrayList<String> cache = new ArrayList<String>();
            
            for(Text value : values){
                str = value.toString().split(":");                    
                term = str[0];
                dfD = Double.parseDouble(str[1]);
                tfD = Double.parseDouble(str[2]);
                normFac += Math.pow(tfD, 2) * Math.pow(Math.log10(totalDocs / dfD), 2);      
                
                cache.add(value.toString());
            }

            StringBuilder list = new StringBuilder(); 
            for (String value : cache) {
                str = value.split(":");
                term = str[0];
                df = str[1];
                tf = str[2];
                
                tfD = Double.parseDouble(tf);
                dfD = Double.parseDouble(df);
                idf = Math.log10(totalDocs / dfD);
                tfidf = tfD * idf / normFac;
                list.append(String.format("%s:%s:%f,", term, df, tfidf));
            }
    
            // remove trailing coma 
            list.deleteCharAt(list.length()-1);
            context.write(key, new Text(list.toString()));
        }
    }

    public static void main(String[] args) throws Exception
    {
        Configuration conf = new Configuration();

        Job job = new Job(conf, "InvertedIndex3");

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
