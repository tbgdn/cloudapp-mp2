import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;
import java.util.StringTokenizer;

// >>> Don't Change
public class OrphanPages extends Configured implements Tool {

    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new Configuration(), new OrphanPages(), args);
        System.exit(res);
    }
// <<< Don't Change

    @Override
    public int run(String[] args) throws Exception {
        //TODO
		Job job = Job.getInstance(this.getConf(), "Orphan Pages");
		job.setOutputKeyClass(Integer.class);
		job.setOutputValueClass(NullWritable.class);

		job.setMapOutputKeyClass(IntWritable.class);
		job.setMapOutputValueClass(IntWritable.class);

		job.setMapperClass(LinkCountMap.class);
		job.setReducerClass(OrphanPageReduce.class);

		FileInputFormat.setInputPaths(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		job.setJarByClass(OrphanPages.class);
		return job.waitForCompletion(true) ? 0 : 1;
    }

    public static class LinkCountMap extends Mapper<Object, Text, IntWritable, IntWritable> {
        @Override
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            //TODO
			System.out.println(key.toString() + " " + value.toString());
			String[] pages = value.toString().split(":");
			if (pages.length >= 1){
				int referral = Integer.valueOf(pages[0].trim());
				context.write(new IntWritable(referral), new IntWritable(0));
			}
			if (pages.length == 2){
				String[] referredPages = pages[1].trim().split(" ");
				for (String referredPage: referredPages){
					context.write(new IntWritable(Integer.valueOf(referredPage)), new IntWritable(1));
				}
			}
		}
    }

    public static class OrphanPageReduce extends Reducer<IntWritable, IntWritable, IntWritable, NullWritable> {
        @Override
        public void reduce(IntWritable key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            //TODO
			int refLinksNum = 0;
			for(IntWritable page: values){
				refLinksNum += page.get();
			}
			if (refLinksNum == 0){
				context.write(key, NullWritable.get());
			}
        }
    }
}