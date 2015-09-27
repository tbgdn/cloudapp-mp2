import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
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

import java.io.IOException;
import java.lang.Integer;
import java.util.StringTokenizer;
import java.util.TreeSet;

// >>> Don't Change
public class TopPopularLinks extends Configured implements Tool {
    public static final Log LOG = LogFactory.getLog(TopPopularLinks.class);

    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new Configuration(), new TopPopularLinks(), args);
        System.exit(res);
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
// <<< Don't Change

    @Override
    public int run(String[] args) throws Exception {
        // TODO
		Configuration conf = this.getConf();
		FileSystem fs = FileSystem.get(conf);
		Path tmpPath = new Path("/mp2/tmp");
		fs.delete(tmpPath, true);

		Job linkCountJob = Job.getInstance(this.getConf(), "Link Count");
		linkCountJob.setOutputKeyClass(IntWritable.class);
		linkCountJob.setOutputValueClass(IntWritable.class);

		linkCountJob.setMapOutputKeyClass(IntWritable.class);
		linkCountJob.setMapOutputValueClass(IntWritable.class);

		linkCountJob.setMapperClass(LinkCountMap.class);
		linkCountJob.setReducerClass(LinkCountReduce.class);

		FileInputFormat.setInputPaths(linkCountJob, new Path(args[0]));
		FileOutputFormat.setOutputPath(linkCountJob, tmpPath);

		linkCountJob.setJarByClass(TopPopularLinks.class);
		linkCountJob.waitForCompletion(true);


		Job jobTopLinks = Job.getInstance(conf, "Top Popular Links");
		jobTopLinks.setOutputKeyClass(IntWritable.class);
		jobTopLinks.setOutputValueClass(IntWritable.class);

		jobTopLinks.setMapOutputKeyClass(NullWritable.class);
		jobTopLinks.setMapOutputValueClass(IntArrayWritable.class);

		jobTopLinks.setMapperClass(TopLinksMap.class);
		jobTopLinks.setReducerClass(TopLinksReduce.class);
		jobTopLinks.setNumReduceTasks(1);

		FileInputFormat.setInputPaths(jobTopLinks, tmpPath);
		FileOutputFormat.setOutputPath(jobTopLinks, new Path(args[1]));

		jobTopLinks.setInputFormatClass(KeyValueTextInputFormat.class);
		jobTopLinks.setOutputFormatClass(TextOutputFormat.class);

		jobTopLinks.setJarByClass(TopPopularLinks.class);
		return jobTopLinks.waitForCompletion(true) ? 0 : 1;
    }

    public static class LinkCountMap extends Mapper<Object, Text, IntWritable, IntWritable> {
        // TODO

		@Override
		protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			String[] pages = value.toString().split(":");
			if (pages.length == 2){
				String[] referredPages = pages[1].trim().split(" ");
				for (String referredPage: referredPages){
					context.write(new IntWritable(Integer.valueOf(referredPage)), new IntWritable(1));
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
			if (refLinksNum == 0){
				context.write(key, new IntWritable(refLinksNum));
			}
		}
	}

    public static class TopLinksMap extends Mapper<Text, Text, NullWritable, IntArrayWritable> {
        Integer N;

        @Override
        protected void setup(Context context) throws IOException,InterruptedException {
            Configuration conf = context.getConfiguration();
            this.N = conf.getInt("N", 10);
        }
        // TODO
		private TreeSet<Pair<Integer, Integer>> topLinks = new TreeSet<Pair<Integer, Integer>>();

		@Override
		protected void map(Text key, Text value, Context context) throws IOException, InterruptedException {
			int linkId = Integer.valueOf(key.toString());
			int refLinksNum = Integer.valueOf(value.toString());
			topLinks.add(new Pair<Integer, Integer>(linkId, refLinksNum));

			if (topLinks.size() > N){
				topLinks.remove(topLinks.first());
			}
			LOG.debug("Mapping something");
		}

		@Override
		protected void cleanup(Context context) throws IOException, InterruptedException {
			for (Pair<Integer, Integer> pair: topLinks){
				context.write(NullWritable.get(), new IntArrayWritable(new Integer[]{pair.first, pair.second}));
			}
		}
	}

    public static class TopLinksReduce extends Reducer<NullWritable, IntArrayWritable, IntWritable, IntWritable> {
        Integer N;

        @Override
        protected void setup(Context context) throws IOException,InterruptedException {
            Configuration conf = context.getConfiguration();
            this.N = conf.getInt("N", 10);
        }
        // TODO

		@Override
		protected void reduce(NullWritable key, Iterable<IntArrayWritable> values, Context context) throws IOException, InterruptedException {
			TreeSet<Pair<Integer, Integer>> topLinks = new TreeSet<Pair<Integer, Integer>>();

			for (IntArrayWritable linkAndCount: values){
				Integer[] ints = (Integer[]) linkAndCount.toArray();
				topLinks.add(new Pair<Integer, Integer>(ints[0], ints[1]));
				if (topLinks.size() > N){
					topLinks.remove(topLinks.first());
				}
			}

			for (Pair<Integer, Integer> pair: topLinks){
				context.write(new IntWritable(pair.first), new IntWritable(pair.second));
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