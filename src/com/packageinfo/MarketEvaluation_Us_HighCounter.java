package com.packageinfo;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Random;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Partitioner;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapred.TextOutputFormat;


public class MarketEvaluation_Us_HighCounter {
	//This program must be executed after executing MarketEvaluation_us.java. The piping is not done here.

	public static class MarketEvalHighCounterMapper extends MapReduceBase implements
	Mapper<LongWritable, Text, Text, IntWritable> {

		@Override
		public void map(LongWritable key, Text value, OutputCollector<Text, IntWritable> output, Reporter reporter)
				throws IOException {
			
			String line = value.toString();
			String[] tokens     = line.split(",");
			String[] secondary_tokens = tokens[2].split("\\t");
			try{
				if (secondary_tokens[1].startsWith("HIGH")){
			output.collect(new Text ("HIGH"), new IntWritable (1));}
			}catch(Exception e) {e.printStackTrace(); System.out.println("Err");}
			}
		}
	
	public static class MarketEvalHighCounterReducer extends MapReduceBase implements
	Reducer<Text, IntWritable, Text, LongWritable> {
		
		public void reduce(Text key, Iterator<IntWritable> values,
				OutputCollector<Text, LongWritable> output, Reporter reporter)
						throws IOException {
			System.out.println("Reducer");
			int sum = 0;
			while (values.hasNext()) {
				sum += values.next().get();
			}
			output.collect(key, new LongWritable (sum));
			//System.out.println("high" + market_count);
		}
	}
 


	public static void main(String[] args) throws Exception {
		// TODO Auto-generated method stub
		JobConf conf = new JobConf(MarketEvaluation_Us_HighCounter.class);
		conf.setJobName("HIGHCount");

		conf.setOutputKeyClass(Text.class);
		conf.setOutputValueClass(IntWritable.class);

		conf.setMapperClass(MarketEvalHighCounterMapper.class);
		conf.setReducerClass(MarketEvalHighCounterReducer.class);

		conf.setInputFormat(TextInputFormat.class);
		conf.setOutputFormat(TextOutputFormat.class);
		//conf.setNumReduceTasks(1);
		Random rand = new Random(95);
		
		
		String input  = "///Users//ramranji//big_data_resources//datasets//op.csv" ;
		String output = "///Users//ramranji//Desktop//" + rand.toString() + "hx";

	FileInputFormat.setInputPaths(conf, new Path(input));
	FileOutputFormat.setOutputPath(conf, new Path(output));

		
	//	FileInputFormat.setInputPaths(conf, new Path(args[0]));
		//FileOutputFormat.setOutputPath(conf, new Path(args[1]));
		JobClient client = new JobClient();
		client.setConf(conf);
		try{
		JobClient.runJob(conf);
		}catch (Exception e) {e.printStackTrace();}
	}

}
