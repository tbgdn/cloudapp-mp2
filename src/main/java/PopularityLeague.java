import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.*;

public class PopularityLeague extends Configured implements Tool {

    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new Configuration(), new PopularityLeague(), args);
        System.exit(res);
    }

    @Override
    public int run(String[] args) throws Exception {
        // TODO
		Configuration conf = this.getConf();
		FileSystem fs = FileSystem.get(conf);
		Path tmpPath = new Path("/mp2/tmp");
		fs.delete(tmpPath, true);

		Job countingJob = Job.getInstance(this.getConf(), "Popularity League");
		countingJob.setOutputKeyClass(IntWritable.class);
		countingJob.setOutputValueClass(IntWritable.class);
		countingJob.setMapOutputKeyClass(IntWritable.class);
		countingJob.setMapOutputValueClass(IntWritable.class);
		countingJob.setMapperClass(LinkCountMap.class);
		countingJob.setReducerClass(LinkCountReduce.class);
		FileInputFormat.setInputPaths(countingJob, new Path(args[0]));
		FileOutputFormat.setOutputPath(countingJob, tmpPath);
		countingJob.setJarByClass(PopularityLeague.class);
		countingJob.waitForCompletion(true);

		Job ranking = Job.getInstance(this.getConf(), "Ranking");
		ranking.setOutputKeyClass(IntWritable.class);
		ranking.setOutputValueClass(IntWritable.class);
		ranking.setMapOutputKeyClass(NullWritable.class);
		ranking.setMapOutputValueClass(IntArrayWritable.class);
		ranking.setMapperClass(PageRankMap.class);
		ranking.setReducerClass(PageRankReduce.class);
		FileInputFormat.setInputPaths(ranking, tmpPath);
		FileOutputFormat.setOutputPath(ranking, new Path(args[1]));
		ranking.setInputFormatClass(KeyValueTextInputFormat.class);
		ranking.setOutputFormatClass(TextOutputFormat.class);
		ranking.setJarByClass(PopularityLeague.class);

		return ranking.waitForCompletion(true) ? 0 : 1;
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

	public static class IntArrayWritable extends ArrayWritable {
		public IntArrayWritable() {
			super(IntWritable.class);
		}

		public IntArrayWritable(Integer[] numbers) {
			super(IntWritable.class);
			IntWritable[] ints = new IntWritable[numbers.length];
			for (int i = 0; i < numbers.length; i++) {
				ints[i] = new IntWritable(numbers[i]);
			}
			set(ints);
		}
	}

	public static class LinkCountMap extends Mapper<Object, Text, IntWritable, IntWritable> {
		// TODO
		private List<String> leagueLinks = new ArrayList<String>();

		@Override
		protected void setup(Context context) throws IOException, InterruptedException {
			Configuration config = context.getConfiguration();
			String leaguePath = config.get("league");
			String leagueContent = readHDFSFile(leaguePath, config);
			for(String line: leagueContent.split("\n")){
				leagueLinks.add(line.trim());
			}
		}

		@Override
		protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			String[] pages = value.toString().split(":");
			if (pages.length == 2){
				String[] referredPages = pages[1].trim().split(" ");
				for (String referredPage: referredPages){
					if (leagueLinks.contains(referredPage)){
						context.write(new IntWritable(Integer.valueOf(referredPage)), new IntWritable(1));
					}
				}
			}
		}
	}

	public static class LinkCountReduce extends Reducer<IntWritable, IntWritable, Text, Text> {
		// TODO

		@Override
		protected void reduce(IntWritable key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
			int refLinksNum = 0;
			for(IntWritable page: values){
				refLinksNum += page.get();
			}
			context.write(new Text(key.toString()), new Text(String.valueOf(refLinksNum)));
		}
	}

	public static class PageRankMap extends Mapper<Text, Text, NullWritable, IntArrayWritable>{

		@Override
		protected void map(Text key, Text value, Context context) throws IOException, InterruptedException {
			Integer pageId = Integer.valueOf(key.toString());
			Integer refsNum = Integer.valueOf(value.toString());
			context.write(NullWritable.get(), new IntArrayWritable(new Integer[]{pageId, refsNum}));
		}
	}

	public static class PageRankReduce extends Reducer<NullWritable, IntArrayWritable, IntWritable, IntWritable>{

		@Override
		protected void reduce(NullWritable key, Iterable<IntArrayWritable> values, Context context) throws IOException, InterruptedException {
			TreeSet<Pair<Integer, Integer>> pageRanks = new TreeSet<Pair<Integer, Integer>>();
			Map<Integer, Integer> pageIdVsRank = new HashMap<Integer, Integer>();
			for (IntArrayWritable pair: values){
				IntWritable[] ints = (IntWritable[])pair.toArray();
				int pageId = ints[0].get();
				int refsNum = ints[1].get();
				pageRanks.add(new Pair<Integer, Integer>(refsNum, pageId));
			}
			int rank = -1;
			int previousCount = 0;
			for (Pair<Integer, Integer> pair: pageRanks){
				int count = pair.first;
				if (count > previousCount) {
					rank ++;
				}
				previousCount = count;
				pageIdVsRank.put(pair.second, rank);
			}
			LOG.debug("Reducing number of inputs.");
			for (IntArrayWritable pair: values){
				IntWritable[] ints = (IntWritable[])pair.toArray();
				int pageId = ints[0].get();
				LOG.info("Writing {} with rank {}.", pageId, pageIdVsRank.get(pageId));
				context.write(new IntWritable(pageId), new IntWritable(pageIdVsRank.get(pageId)));
			}
		}
	}
}

// >>> Don't Change
class Pair<A extends Comparable<? super A>,
				  B extends Comparable<? super B>>
		implements Comparable<Pair<A, B>> {

	public final A first;
	public final B second;

	public Pair(A first, B second) {
		this.first = first;
		this.second = second;
	}

	public static <A extends Comparable<? super A>,
						  B extends Comparable<? super B>>
	Pair<A, B> of(A first, B second) {
		return new Pair<A, B>(first, second);
	}

	@Override
	public int compareTo(Pair<A, B> o) {
		int cmp = o == null ? 1 : (this.first).compareTo(o.first);
		return cmp == 0 ? (this.second).compareTo(o.second) : cmp;
	}

	@Override
	public int hashCode() {
		return 31 * hashcode(first) + hashcode(second);
	}

	private static int hashcode(Object o) {
		return o == null ? 0 : o.hashCode();
	}

	@Override
	public boolean equals(Object obj) {
		if (!(obj instanceof Pair))
			return false;
		if (this == obj)
			return true;
		return equal(first, ((Pair<?, ?>) obj).first)
					   && equal(second, ((Pair<?, ?>) obj).second);
	}

	private boolean equal(Object o1, Object o2) {
		return o1 == o2 || (o1 != null && o1.equals(o2));
	}

	@Override
	public String toString() {
		return "(" + first + ", " + second + ')';
	}
}
// <<< Don't Change