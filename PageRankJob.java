package pagerank;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.UnsupportedEncodingException;
import java.net.URLDecoder;
import java.util.ArrayList;
import java.util.Date;
import java.util.LinkedList;
import java.util.Queue;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class PageRankJob {

	public class HandlerThread implements Runnable{

		String input_bucket;
		int num = 0;
		S3Wrapper s3;
		Queue<Set<String>> shared_sets = new LinkedList<Set<String>>();

		public HandlerThread(String str) {
			input_bucket = str;
			s3 = new S3Wrapper(input_bucket);
			while(s3.isTruncated()) {
				shared_sets.add(s3.keySet());
			}
		}

		public synchronized Set<String> get_set() {
			if (!shared_sets.isEmpty()) {
				return shared_sets.remove();
			}
			return null;
		}

		public void run() {
			File f = new File("./inputs/input_file_"+ Thread.currentThread().getId() +".txt");
			PrintWriter writer = null;
			try {
				writer = new PrintWriter(f, "UTF-8");
			} catch (FileNotFoundException e1) {
				e1.printStackTrace();
			} catch (UnsupportedEncodingException e1) {
				e1.printStackTrace();
			}
			Set<String> keySet;
			while ((keySet=get_set())!=null) {
				for (String key: keySet) {
					try {
						try {
							writer.println(URLDecoder.decode(key, "UTF-8") + "\t" + URLDecoder.decode(s3.get(key), "UTF-8"));
						} catch (UnsupportedEncodingException e) {
							e.printStackTrace();
						}
					}
					catch(IllegalArgumentException e) {
						System.out.println(key + "---->" + s3.get(key));
					}
				}
			}
			writer.close();
		}
	}

	public void concatenate_all_files() throws IOException {
		PrintWriter fw = new PrintWriter("./input_file.txt");
		for (File f : new File("./inputs").listFiles()) {
			BufferedReader br = new BufferedReader(new FileReader(f));
			String line = br.readLine();
			while(line != null) {
				fw.println(line);
				line = br.readLine();
			}
			br.close();
		}
		fw.close(); 
	}

	/**
	 * Function to run a mapreduce job
	 * @param Mapper the mapper's class
	 * @param Reducer the reducer's class
	 * @param input_path the path to the input file
	 * @param output_path the path where the output directory will be stored
	 * @return the mapreduce job
	 * @throws IllegalArgumentException
	 * @throws IOException
	 */
	public Job runJob(Class Mapper, Class Reducer, String input_path, String output_path) throws IllegalArgumentException, IOException {
		Configuration conf = new Configuration();
		conf.set("fs.file.impl", org.apache.hadoop.fs.LocalFileSystem.class.getName());
		@SuppressWarnings("deprecation")
		Job job = new Job(conf, "pagerank");
		job.setJarByClass(PageRankJob.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		job.setInputFormatClass(KeyValueTextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		job.setMapperClass(Mapper);
		job.setReducerClass(Reducer);
		FileInputFormat.addInputPath(job, new Path(input_path));
		FileOutputFormat.setOutputPath(job, new Path(output_path));
		return job;
	}

	/**
	 * The main function accepts three optional command line arguments
	 * First argument - The number of iterations to run the job for
	 * Second argument - The path to the input file
	 * Third argument - The path to store the outputs
	 * @param args
	 * @throws IOException
	 * @throws ClassNotFoundException
	 * @throws InterruptedException
	 */
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {

		int num_of_iterations = 0;
		String input_path = "";
		String output_path = "";
		String intermediate_output_path = "";
		PageRankJob pagerank = new PageRankJob();
		new File("./inputs").mkdir();
		String input_bucket;
		String output_table;
		int write_throughput;
		int read_throughput;
		if (args.length >= 1)
			input_bucket = args[0];
		else
			input_bucket = "links4.28";
		if (args.length >= 2)
			output_table = args[1];
		else
			output_table = "pagerank4.28_alt";
		if (args.length >= 3)
			read_throughput = Integer.parseInt(args[2]);
		else
			read_throughput = 50;
		if (args.length >= 4)
			write_throughput = Integer.parseInt(args[3]);
		else
			write_throughput = 1000;
		ArrayList<Thread> threadpool = new ArrayList<Thread>();
		HandlerThread handler = pagerank.new HandlerThread(input_bucket);
		for (int i = 0; i< 16; i++) {
			Thread t = new Thread(handler);
			threadpool.add(t);
			t.start();
		}
		while(true) {
			boolean flag = true;
			for (Thread t : threadpool) {
				if (t.getState().toString().equals("RUNNABLE")) {
					flag = false;
				}
			}
			if (flag)
				break;
		}

		System.out.println("All input files are created on each thread\n");

		pagerank.concatenate_all_files();

		if (args.length >= 5)
			num_of_iterations = Integer.parseInt(args[4]);
		else
			num_of_iterations = 10;
		input_path = "./input_file.txt";
		output_path = "./outputs/output";
		intermediate_output_path = output_path.substring(0, output_path.lastIndexOf('/'))+"/intermediate_outputs";
		
		long a = new Date().getTime();
		Job job = pagerank.runJob(Mapper1.class, Reducer1.class, input_path, intermediate_output_path+"_job1");
		while(!job.waitForCompletion(true)){}
		long b = new Date().getTime();
		System.out.println("First job done in "+(b-a)/6000+" mins");

		a = new Date().getTime();
		job = pagerank.runJob(Mapper2.class, Reducer2.class, intermediate_output_path+"_job1", intermediate_output_path+"_job2");
		while(!job.waitForCompletion(true)){}
		b = new Date().getTime();

		System.out.println("Second job done in "+(b-a)/6000+" mins");

		a = new Date().getTime();
		for(int i = 0; i<num_of_iterations; i++) {
			System.out.println("Iteration "+(i+1));
			if(i == 0) {
				job = pagerank.runJob(PageRankMapper.class, PageRankReducer.class, intermediate_output_path+"_job2", intermediate_output_path+"_iteration_"+i);
			}
			else if(i == num_of_iterations -1){
				int j = i-1;
				job = pagerank.runJob(PageRankMapper.class, PageRankReducer.class, intermediate_output_path+"_iteration_"+j, output_path);
			}
			else {
				int j = i-1;
				job = pagerank.runJob(PageRankMapper.class, PageRankReducer.class, intermediate_output_path+"_iteration_"+j, intermediate_output_path+"_iteration_"+i);
			}
			while(!job.waitForCompletion(true)){}
		}

		//Get the page ranks of all the links
		PrintWriter fw = new PrintWriter("./page_ranks.txt");
		BufferedReader br = new BufferedReader(new FileReader("./outputs/output/part-r-00000"));
		String line = br.readLine();
		while(line != null) {
			String[] line_parts = line.split("::::", 2);
			fw.println(line_parts[0].toString() + "\t" + line_parts[1].split("\t",2)[0].toString());
			line = br.readLine();
		}
		br.close();
		fw.close();

		DynamoDBWrapper db = new DynamoDBWrapper(output_table, read_throughput, write_throughput, null);
		BufferedReader br2 = new BufferedReader(new FileReader("./page_ranks.txt"));
		String line_new = br2.readLine();
		while(line_new != null) {
			String[] line_parts = line_new.split("\t", 2);
			db.put(line_parts[0], "pagerank", line_parts[1]);
			line_new = br2.readLine();
		}
		br2.close();
		b = new Date().getTime();
		System.out.println("PAGE RANK ITERATIONS DONE IN "+(b-a)/6000+" mins");
	}

}