// adaptado de https://timepasstechies.com/map-reduce-inverted-index-sample/

import java.io.IOException;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import java.util.StringTokenizer;
import java.io.File;
import org.apache.commons.io.FileUtils;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class InvertedIndex {

	public static class InvertedIndexMapper extends Mapper<Object, Text, Text, Text> {

		private Text word = new Text();
		private Text fileNameValue = new Text();		

		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			
			StringTokenizer itr = new StringTokenizer(value.toString().replaceAll("[,.()\"“:;?!º%=/]", "").toLowerCase());
			String fileName = ((FileSplit) context.getInputSplit()).getPath().getName();
			fileNameValue.set(fileName);	
			while (itr.hasMoreTokens()) {
				word.set(itr.nextToken());						
				context.write(word, fileNameValue);
			}
		}
	}
	
	public static class InvertedIndexReducer extends Reducer<Text, Text, Text, Text> {

		private Text result = new Text();

		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

			StringBuilder sb = new StringBuilder();
			boolean first = true;
			for (Text value : values) {

				if (first) {
					first = false;
				} else {
					sb.append(" ");
				}

				if (sb.lastIndexOf(value.toString()) < 0) {
					sb.append(value.toString());
				}

			}
			result.set(sb.toString());

			context.write(key, result);

		}

	}

	public static void main(String[] args) throws Exception {

		Job job = Job.getInstance(new Configuration(), "inverted index");
        job.setJarByClass(InvertedIndex.class);
		
		job.setMapperClass(InvertedIndexMapper.class);
		job.setReducerClass(InvertedIndexReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);

	}	
	
}
