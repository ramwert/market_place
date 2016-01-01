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
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapred.TextOutputFormat;


public class MarketEvaluation_us {
	//DATA FORMAT
//	FMID,MarketName,Website,Street,City,County,State,Zip,Schedule,x,y,Location,
//	Credit,WIC,WICcash,SFMNP,SNAP,Bakedgoods,Cheese,Crafts,Flowers,Eggs,Seafood,Herbs,Vegetables,Honey,Jams,Maple,Meat,Nursery,Nuts,Plants,Poultry,Prepared,Soap,Trees,Wine,updateTime
//	1002267,"""Y Not Wednesday Farmers Market at Town Center""",http://www.sandlercenter.org/index/ynotwednesdays,201 Market Street,Virginia Beach,Virginia Beach,Virginia,23462,June - August Wednesday 5:00 PM to 8:00 PM,-76.135361,36.841885,Other,
//	Y,N,N,N,N,Y,Y,N,Y,Y,Y,N,Y,Y,Y,N,N,N,N,N,N,Y,Y,N,Y,5/5/2012 17:56

	//Variable for filter, defaults to :all which means all the county.for future.
	static private String county = ":all";
	
	MarketEvaluation_us (String county_filter){
		county = county_filter;
	}
		
	public static class MarketEvalMapper extends MapReduceBase implements
	Mapper<LongWritable, Text, Text, IntWritable> {
		
		//Function to count number of Y in the product.
		  private HashMap<String,Integer> countStringOccurences(String[] strArray) {
		        HashMap<String, Integer> countMap = new HashMap<String, Integer>();
		        for (String string : strArray) {
		            if (!countMap.containsKey(string)) {
		                countMap.put(string.toUpperCase(), 1);
		            } else {
		                Integer count = countMap.get(string);
		                count = count + 1;
		                countMap.put(string.toUpperCase(), count);
		            }
		        }
		     
		        return countMap;
 		    }
		 
		  //Function to separate the product list from rest of elements.
		  private String [] createStringArray (String[] values,int start,int end){
			  String temp[] = new String [(end-start) + 1];
			  for (int i =start,k=0;i<=end;i++,k++){
				  temp [k]=values[i];
			  }
			  return temp;
		  }
		  
		
		@Override
		public void map(LongWritable key, Text value, OutputCollector<Text, IntWritable> output, Reporter reporter)
				throws IOException {
			
			String line = value.toString();
			String[] tokens = line.split(",");
			if  (!tokens [0].toUpperCase().equals("FMID")) //Ignoring row header
			{
			String market = tokens[1].toUpperCase();
			String county = tokens[5].toUpperCase(); //Fetching county here.
			String market_county = county + "," + market;
			String[] products_list  = createStringArray(tokens,12,36);//column 12-36 are product list
			HashMap<String,Integer> map = countStringOccurences(products_list);
			int number_of_Y = (map.containsKey("Y"))? map.get("Y"):0;
			System.out.println(market);
			try{
			output.collect(new Text (market_county), new IntWritable(number_of_Y));
			}catch(Exception e) {e.printStackTrace(); System.out.println("Err");}
			}
		}
	}
	
	public static class MarketEvalReducer extends MapReduceBase implements
	Reducer<Text, IntWritable, Text, Text> {
		
		private int market_count = 0;
		private String categoriseMarket(int value){
			float percentage = (float) ( (value / 25.0) * 100.0);
			if (percentage > 60) return "HIGH";
			else if (percentage >40 && percentage <=60) return "MEDIUM";
			else if (percentage <= 40) return "LOW";
			else return "FAULT";
		}
		
		
		public void reduce(Text key, Iterator<IntWritable> values,
				OutputCollector<Text, Text> output, Reporter reporter)
						throws IOException {
			System.out.println("Reducer");
			int sum = 0;
			while (values.hasNext()) {
				sum += values.next().get();
			}
			String categorisation =  categoriseMarket(sum);
			//String output_value   =  categorisation +"(" +sum + ")";
			System.out.println(key);
			output.collect(key, new Text ("," + categorisation));
		}
	}
 

	public static void main(String[] args) throws Exception {
		// TODO Auto-generated method stub
		JobConf conf = new JobConf(MarketEvaluation_us.class);
		conf.setJobName("Productcount");

		conf.setOutputKeyClass(Text.class);
		conf.setOutputValueClass(IntWritable.class);

		conf.setMapperClass(MarketEvalMapper.class);
		conf.setReducerClass(MarketEvalReducer.class);

		conf.setInputFormat(TextInputFormat.class);
		conf.setOutputFormat(TextOutputFormat.class);
		Random rand = new Random(95);
		
		
		//String input  = "///Users//ramranji//big_data_resources//datasets//sample.csv" ;
		//String output = "///Users//ramranji//Desktop//" + rand.toString() + "dfdaddf34t";

		//FileInputFormat.setInputPaths(conf, new Path(input));
//	    FileOutputFormat.setOutputPath(conf, new Path(output));

		
		FileInputFormat.setInputPaths(conf, new Path(args[0]));
		FileOutputFormat.setOutputPath(conf, new Path(args[1]));
		JobClient client = new JobClient();
		client.setConf(conf);
		try{
		JobClient.runJob(conf);
		}catch (Exception e) {e.printStackTrace();}
	}

}
