/*
 * Basado en la implementación de Daniele Pantaleone, https://github.com/danielepantaleone/hadoop-pagerank
 * La carpeta output no debe existir en el s3 de aws
 * Parámetros de pagerank.jar PageRank: 0.8 50 s3://../input s3://../output
*/

import java.io.IOException;
import java.text.DecimalFormat;
import java.text.NumberFormat;
import java.util.HashSet;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.Job;

import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

public class PageRank {

    public static Set<String> NODOS = new HashSet<String>();
    public static NumberFormat NF = new DecimalFormat("00");
    public static String LINKS_SEPARATOR = "|";
    
    public static Double DAMPING=0.8;
    public static int ITERACIONES=1;
    public static String IN_PATH="";
    public static String OUT_PATH="";
   
    public static void main(String[] args) throws Exception {
		
        DAMPING = Double.parseDouble(args[0]);
        ITERACIONES = Integer.parseInt(args[1]);
        IN_PATH = args[2];
        OUT_PATH = args[3];
        
        //job 1
        
        Job job = Job.getInstance(new Configuration(), "Job 1");
        job.setJarByClass(PageRank.class);
            
        FileInputFormat.addInputPath(job, new Path(IN_PATH));
        job.setInputFormatClass(TextInputFormat.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setMapperClass(PageRankJob1Mapper.class);
        // nodoFrom nodoTo
        
        FileOutputFormat.setOutputPath(job, new Path(OUT_PATH + "/iter00"));
        job.setOutputFormatClass(TextOutputFormat.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        job.setReducerClass(PageRankJob1Reducer.class);
        // nodo rank link1,link2,link3
          
		if (!job.waitForCompletion(true))
			System.exit(1);
        
        //job 2
        
        String in = null;
        String out = null;
        for (int i = 0; i < ITERACIONES; i++) {
			
            in = OUT_PATH + "/iter" + NF.format(i);
            out = OUT_PATH + "/iter" + NF.format(i + 1);		
			
			job = Job.getInstance(new Configuration(), "Job 2");
			job.setJarByClass(PageRank.class);			
			
			FileInputFormat.setInputPaths(job, new Path(in));
			job.setInputFormatClass(TextInputFormat.class);
			job.setMapOutputKeyClass(Text.class);
			job.setMapOutputValueClass(Text.class);
			job.setMapperClass(PageRankJob2Mapper.class);
			// link1 rankNodo cantLinksdeNodo
			// link2 rankNodo cantLinksdeNodo
			// link3 rankNodo cantLinksdeNodo
			// nodo |link1,link2,link3			
			
			FileOutputFormat.setOutputPath(job, new Path(out));
			job.setOutputFormatClass(TextOutputFormat.class);
			job.setOutputKeyClass(Text.class);
			job.setOutputValueClass(Text.class);
			job.setReducerClass(PageRankJob2Reducer.class);
			// nodo rankNuevo link1,link2,link3
				
			if (!job.waitForCompletion(true))
				System.exit(1);
        }
        
        // Job 3
        
		job = Job.getInstance(new Configuration(), "Job 3");
        job.setJarByClass(PageRank.class);        
        
        FileInputFormat.setInputPaths(job, new Path(out));
        job.setInputFormatClass(TextInputFormat.class);
        job.setMapOutputKeyClass(LongWritable.class);
        job.setMapOutputValueClass(DoubleWritable.class);
        job.setMapperClass(PageRankJob3Mapper.class);
        // nodo rank
        
        FileOutputFormat.setOutputPath(job, new Path(OUT_PATH + "/result"));
        job.setOutputFormatClass(TextOutputFormat.class);
        job.setOutputKeyClass(LongWritable.class);
        job.setOutputValueClass(DoubleWritable.class);
        
        if (!job.waitForCompletion(true))
			System.exit(1);
		
        System.exit(0);
    }
    
	public static class PageRankJob1Mapper extends Mapper<LongWritable, Text, Text, Text> {

		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			
			int tabIndex = value.find("\t");
			String nodeA = Text.decode(value.getBytes(), 0, tabIndex);
			String nodeB = Text.decode(value.getBytes(), tabIndex + 1, value.getLength() - (tabIndex + 1));
			context.write(new Text(nodeA), new Text(nodeB));
						
			NODOS.add(nodeA);
			NODOS.add(nodeB);
			
		}
	}


	public static class PageRankJob1Reducer extends Reducer<Text, Text, Text, Text> {

		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
					
			String links = "1.0" + "\t";
			
			boolean coma = false;
			for (Text value : values) {
				if (coma) 
					links += ",";
				links += value.toString();								
				coma = true;
			}

			context.write(key, new Text(links));
		}

	}

	public static class PageRankJob2Mapper extends Mapper<LongWritable, Text, Text, Text> {
		
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			
			int idxNodo = value.find("\t");
			int idxRank = value.find("\t", idxNodo + 1);
			
			String nodo = Text.decode(value.getBytes(), 0, idxNodo);
			String rank = Text.decode(value.getBytes(), idxNodo + 1, idxRank - (idxNodo + 1));
			String linksComas = Text.decode(value.getBytes(), idxRank + 1, value.getLength() - (idxRank + 1));
			
			String[] links = linksComas.split(",");
			for (String link : links) { 
				Text rankLinks = new Text(rank + "\t" + links.length);
				context.write(new Text(link), rankLinks);
			}
			
			context.write(new Text(nodo), new Text(LINKS_SEPARATOR + linksComas));			
		}		
	}


	public static class PageRankJob2Reducer extends Reducer<Text, Text, Text, Text> {
		
		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, 
																					InterruptedException {
			
			String links = "";
			double sumShareOtherPageRanks = 0.0;
			
			for (Text value : values) {
	 
				String content = value.toString();
				
				if (content.startsWith(LINKS_SEPARATOR)) {
					// if this value contains node links append them to the 'links' string
					// for future use: this is needed to reconstruct the input for Job#2 mapper
					// in case of multiple iterations of it.
					links += content.substring(LINKS_SEPARATOR.length());
				} else {
					
					String[] split = content.split("\\t");
					
					double pageRank = Double.parseDouble(split[0]);
					int totalLinks = Integer.parseInt(split[1]);
					
					// add the contribution of all the pages having an outlink pointing 
					// to the current node: we will add the DAMPING factor later when recomputing
					// the final pagerank value before submitting the result to the next job.
					sumShareOtherPageRanks += (pageRank / totalLinks);
				}

			}
			
			double newRank = DAMPING * sumShareOtherPageRanks + (1 - DAMPING);
			context.write(key, new Text(newRank + "\t" + links));
			
		}

	}


	public static class PageRankJob3Mapper extends Mapper<LongWritable, Text, LongWritable, DoubleWritable> {
		
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			
			int idxNodo = value.find("\t");
			int idxRank = value.find("\t", idxNodo+1);
			
			String nodo = Text.decode(value.getBytes(), 0, idxNodo);
			double pageRank = Double.parseDouble(Text.decode(value.getBytes(), idxNodo+1, idxRank - (idxNodo+1)));
			
			context.write(new LongWritable(Long.parseLong(nodo)), new DoubleWritable(pageRank));			
		}		   
	}	
	

}
