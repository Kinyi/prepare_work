package hbase;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.mapreduce.TableOutputFormat;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;

public class HBaseImport {

	public static void main(String[] args) throws Exception {
		// TODO Auto-generated method stub
		Configuration conf = HBaseConfiguration.create();
		conf.set("hbase.zookeeper.quorum", "hadoop0");
		conf.set("dfs.socket.timeout", "180000");
		conf.set(TableOutputFormat.OUTPUT_TABLE, "wlan");
		
		Job job = new Job(conf, HBaseImport.class.getSimpleName());
		job.setJarByClass(HBaseImport.class);
		job.setMapperClass(BatchImportMapper.class);
		job.setReducerClass(BatchImportReducer.class);
		job.setMapOutputKeyClass(LongWritable.class);
		job.setMapOutputValueClass(Text.class);
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TableOutputFormat.class);
		FileInputFormat.setInputPaths(job, new Path("hdfs://hadoop0:9000/input/"));
		job.waitForCompletion(true);
	}
	
	static class BatchImportMapper extends Mapper<LongWritable, Text, LongWritable , Text>{
		SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyyMMddHHmmss");
		Text v2 = new Text();
		@Override
		protected void map(LongWritable key, Text value,
				Mapper<LongWritable, Text, LongWritable, Text>.Context context)
				throws IOException, InterruptedException {
			try {
				String[] split = value.toString().split("\t");
				Date date = new Date(Long.parseLong(split[0].trim()));
				String formated = simpleDateFormat.format(date);
				v2.set(split[1]+":"+formated+"\t"+value.toString());
				context.write(key, v2);
			} catch (NumberFormatException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
	}
	
	static class BatchImportReducer extends TableReducer<LongWritable , Text, NullWritable>{
		@Override
		protected void reduce(LongWritable k2, Iterable<Text> v2s,
				Reducer<LongWritable, Text, NullWritable, Writable>.Context context)
				throws IOException, InterruptedException {
			for (Text text : v2s) {
				String[] split = text.toString().split("\t");
				Put put = new Put(split[0].getBytes());
				put.add("cf".getBytes(), "reportTime".getBytes(), split[1].getBytes());
				put.add("cf".getBytes(), "msisdn".getBytes(), split[2].getBytes());
				put.add("cf".getBytes(), "apmac".getBytes(), split[3].getBytes());
				put.add("cf".getBytes(), "acmac".getBytes(), split[4].getBytes());
				put.add("cf".getBytes(), "host".getBytes(), split[5].getBytes());
				put.add("cf".getBytes(), "siteType".getBytes(), split[6].getBytes());
				put.add("cf".getBytes(), "upPackNum".getBytes(), split[7].getBytes());
				put.add("cf".getBytes(), "downPackNum".getBytes(), split[8].getBytes());
				put.add("cf".getBytes(), "upPayLoad".getBytes(), split[9].getBytes());
				put.add("cf".getBytes(), "downPayLoad".getBytes(), split[10].getBytes());
				put.add("cf".getBytes(), "httpStatus".getBytes(), split[11].getBytes());
				
				context.write(NullWritable.get(), put);
			}
		}
	}

}
