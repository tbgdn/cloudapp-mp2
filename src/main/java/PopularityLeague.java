import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class PopularityLeague extends Configured implements Tool {

    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new Configuration(), new PopularityLeague(), args);
        System.exit(res);
    }

    @Override
    public int run(String[] args) throws Exception {
        // TODO
		Job job = Job.getInstance(this.getConf(), "Popularity League");
		job.setOutputKeyClass(IntWritable.class);
		job.setOutputValueClass(IntWritable.class);

		job.setMapOutputKeyClass(IntWritable.class);
		job.setMapOutputValueClass(IntWritable.class);

		job.setMapperClass(LinkCountMap.class);
		job.setReducerClass(LinkCountReduce.class);

		FileInputFormat.setInputPaths(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		job.setJarByClass(TitleCount.class);
		return job.waitForCompletion(true) ? 0 : 1;
    }

    // TODO

	private static Logger LOG = LoggerFactory.getLogger(PopularityLeague.class);

	public static String readHDFSFile(String path, Configuration conf) throws IOException{
		Path pt=new Path(path);
		FileSystem fs = FileSystem.get(pt.toUri(), conf);
		FSDataInputStream file = fs.open(pt);
		BufferedReader buffIn=new BufferedReader(new InputStreamReader(file));

		StringBuilder everything = new StringBuilder();
		String line;
		while( (line = buffIn.readLine()) != null) {
			everything.append(line);
			everything.append("\n");
		}
		return everything.toString();
	}

	public static class LinkCountMap extends Mapper<Object, Text, IntWritable, IntWritable> {
		// TODO
		private List<String> league = new ArrayList<String>();

		@Override
		protected void setup(Context context) throws IOException, InterruptedException {
			String leaguePath = context.getConfiguration().get("league");
			league = Arrays.asList(readHDFSFile(leaguePath, context.getConfiguration()).split("\n"));
			LOG.error("Here is the league: ", league);
		}

		@Override
		protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			String[] pages = value.toString().split(":");
			if (pages.length == 2){
				String[] referredPages = pages[1].trim().split(" ");
				for (String referredPage: referredPages){
					if (league.contains(league)){
						context.write(new IntWritable(Integer.valueOf(referredPage)), new IntWritable(1));
					}
				}
			}
		}
	}

	public static class LinkCountReduce extends Reducer<IntWritable, IntWritable, IntWritable, IntWritable> {
		// TODO

		@Override
		protected void reduce(IntWritable key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
			int refLinksNum = 0;
			for(IntWritable page: values){
				refLinksNum += page.get();
			}
			context.write(key, new IntWritable(refLinksNum));
		}
	}

}