package edu.pitt.sis.infsci2711.friendcount;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.StringTokenizer;
import java.util.TreeMap;
import java.util.Comparator;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class FriendCount extends Configured implements Tool {
	public static void main(String[] args) throws Exception {
		System.out.println(Arrays.toString(args));
		int res = ToolRunner.run(new Configuration(), new FriendCount(), args);

		System.exit(res);
	}
	
	
	@Override
	public int run(String[] args) throws Exception {
		System.out.println(Arrays.toString(args));
		@SuppressWarnings("deprecation")
		Job job = new Job(getConf(), "FriendCount");
		job.setJarByClass(FriendCount.class);
		job.setOutputKeyClass(LongWritable.class);
		job.setOutputValueClass(FriendCountWritable.class);

		job.setMapperClass(Map.class);
		job.setReducerClass(Reduce.class);

		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);

		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		job.waitForCompletion(true);

		return 0;
	}

	/**
	 * This is the class that defines the value type.
	 */
	static public class FriendCountWritable implements Writable {
		public Long user;
		public Long mutualFriend;

		public FriendCountWritable(Long user, Long mutualFriend) {
			this.user = user;
			this.mutualFriend = mutualFriend;
		}

		public FriendCountWritable() {
			this(-1L, -1L);
		}

		@Override
		public void write(DataOutput out) throws IOException {
			out.writeLong(user);
			out.writeLong(mutualFriend);
		}

		@Override
		public void readFields(DataInput in) throws IOException {
			user = in.readLong();
			mutualFriend = in.readLong();
		}

		@Override
		public String toString() {
			return " toUser: " + Long.toString(user) + " mutualFriend: "
					+ Long.toString(mutualFriend);
		}
	}

	/**
	 * This class is to mapping.
     * LongWritable
     * Text - the type that need to be analyzed.
     * LongWritable - the key type of the map outcome.
     * FriendCountWritable - the value type of the map outcome.
	 * the format of the outcome : <user recommendedfriend mutualfriend>
	 */
	public static class Map extends
			Mapper<LongWritable, Text, LongWritable, FriendCountWritable> {

		@Override
		public void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			//deal with the text line by line
			//line[0]= the user; line[1]= user's friends
			String line[] = value.toString().split("\t");
			Long fromUser = Long.parseLong(line[0]);
			List<Long> toUsers = new ArrayList<Long>();

			if (line.length == 2) {
				StringTokenizer tokenizer = new StringTokenizer(line[1], ",");
				while (tokenizer.hasMoreTokens()) {
					Long toUser = Long.parseLong(tokenizer.nextToken());
					toUsers.add(toUser);
					//write the record that two persons have already been friends using value -1L; < fromUser, toUser -1L>
					context.write(new LongWritable(fromUser),
							new FriendCountWritable(toUser, -1L));
				}

				//write the records that two persons have the same friend; <toUser[i] toUser[j] fromUser> && <toUser[j] toUser[i] fromUser>
				for (int i = 0; i < toUsers.size(); i++) {
					for (int j = i + 1; j < toUsers.size(); j++) {
						context.write(new LongWritable(toUsers.get(i)),
								new FriendCountWritable((toUsers.get(j)),
										fromUser));
						context.write(new LongWritable(toUsers.get(j)),
								new FriendCountWritable((toUsers.get(i)),
										fromUser));
					}
				}
			}
		}
	}

	/**
	 * This class is to reducing.
	 */
	public static class Reduce extends
			Reducer<LongWritable, FriendCountWritable, LongWritable, Text> {
		 /**
		    * This method is to reduce by calculating values according to the same friend.
		    * @param key - the key from the map outcome.
		    * @param values - key's values
		    * @param - reduce outcome.
		    */
		@Override
		public void reduce(LongWritable key,
				Iterable<FriendCountWritable> values, Context context)
				throws IOException, InterruptedException {

			// key is the recommended friend, and value is the list of mutual friends
			final java.util.Map<Long, List<Long>> mutualFriends = new HashMap<Long, List<Long>>();

			//calculate the values by iterate FriendCountWritable
			for (FriendCountWritable val : values) {
				final Boolean isAlreadyFriend = (val.mutualFriend == -1);
				final Long toUser = val.user;
				final Long mutualFriend = val.mutualFriend;

				//check whether the recommended friend has been dealt with before
				if (mutualFriends.containsKey(toUser)) {
					//if exists, check whether they are friends already.
					if (isAlreadyFriend) {
						mutualFriends.put(toUser, null);
					} else if (mutualFriends.get(toUser) != null) {//if they are not friend and already recommended, add the mutual friend
						mutualFriends.get(toUser).add(mutualFriend);
					}
				} else {
					//create the recommended record to add the recommended friend and the mutual friend
					if (!isAlreadyFriend) {
						mutualFriends.put(toUser, new ArrayList<Long>() {
							{
								add(mutualFriend);
							}
						});
					} else {
						mutualFriends.put(toUser, null);
					}
				}
			}

			//sort; ordered in the decreasing number of mutual friends
			java.util.SortedMap<Long, List<Long>> sortedMutualFriends = new TreeMap<Long, List<Long>>(
					new Comparator<Long>() {
						@Override
						public int compare(Long key1, Long key2) {
							Integer v1 = mutualFriends.get(key1).size();
							Integer v2 = mutualFriends.get(key2).size();
							if (v1 > v2) {
								return -1;
							} else if (v1.equals(v2) && key1 < key2) {
								return -1;
							} else {
								return 1;
							}
						}
					});

			for (java.util.Map.Entry<Long, List<Long>> entry : mutualFriends
					.entrySet()) {
				if (entry.getValue() != null) {
					sortedMutualFriends.put(entry.getKey(), entry.getValue());
				}
			}

			Integer i = 0;
			String output = "";
			for (java.util.Map.Entry<Long, List<Long>> entry : sortedMutualFriends
					.entrySet()) {
				if (i < 10) {
					if (i == 0) {
						output = entry.getKey().toString() + " ("
								+ entry.getValue().size() + ")";
					} else {
						output += "," + entry.getKey().toString() + " ("
								+ entry.getValue().size() + ")";
					}
					++i;
				} else {
					break;
				}
			}
			context.write(key, new Text(output));
		}
	}

	

}