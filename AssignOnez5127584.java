import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.MapWritable;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;










public class AssignOnez5127584 {
	
	public static class MyMap extends MapWritable{

		@Override
		public String toString() {
			String ret ="";
			for (Map.Entry e: this.entrySet())
				ret += e.getKey().toString() + e.getValue().toString();
			return ret;
		}	
	}
	public static class MovieWritable implements Writable {

		private IntWritable rating_1;
		private Text user;
		private IntWritable rating_2;
		
		public MovieWritable() {
			this.user = new Text("");
			this.rating_1 = new IntWritable(-1);
			this.rating_2 = new IntWritable(-1);
			
		}
		
		public MovieWritable(IntWritable rating_1, IntWritable rating_2, Text user) {
			super();
			this.user = user;
			this.rating_1 = rating_1;
			this.rating_2 = rating_2;
			
		}


		public IntWritable getRating_1() {
			return rating_1;
		}

		public void setRating_1(IntWritable rating_1) {
			this.rating_1 = rating_1;
		}

		public IntWritable getRating_2() {
			return rating_2;
		}

		public void setRating_2(IntWritable rating_2) {
			this.rating_2 = rating_2;
		}
		
		public Text getUser() {
			return user;
		}

		public void setUser(Text user) {
			this.user = user;
		}
		@Override
		public void readFields(DataInput data) throws IOException {
			this.user.readFields(data);
			this.rating_1.readFields(data);
			this.rating_2.readFields(data);
			
		}

		@Override
		public void write(DataOutput data) throws IOException {
			this.user.write(data);
			this.rating_1.write(data);
			this.rating_2.write(data);
			
		}
		
		public String toString() {
			return this.user.toString() +","+this.rating_1.toString()+","+this.rating_2.toString();
		}

		
		
	}
	
//	public class StringArrayWritable extends ArrayWritable {
//		public StringArrayWritable() {
//		    super(Text.class);
//		}
//
//		public StringArrayWritable(Text[] values){
//		    super(Text.class, values);
//		    Text[] texts = new Text[values.length];
//		    for (int i = 0; i < values.length; i++) {
//		        texts[i] = new Text(values[i]);
//		    }
//		    set(texts);
//		}
//
//		@Override
//		public String toString(){
//		    StringBuilder sb = new StringBuilder();
//
//		    for(String s : super.toStrings()){
//		        sb.append(s).append("\t");
//		    }
//
//		    return sb.toString();
//		}
//
//		public void set(Object[] array) {
//			// TODO Auto-generated method stub
//			
//		}
//		}	
	static class LongArrayWritable extends ArrayWritable {
		 
		public LongArrayWritable() {
			super(LongWritable.class);
		}
 
		public LongArrayWritable(String[] string) {
			super(LongWritable.class);
			LongWritable[] longs = new LongWritable[string.length];
			for (int i = 0; i < longs.length; i++) {
				longs[i] = new LongWritable(Long.valueOf(string[i]));
			}
			set(longs);
		}
	}


	
	public static class MovieMapper extends Mapper<LongWritable, Text, Text, MyMap>{


		@Override
		protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, MyMap>.Context context)
				throws IOException, InterruptedException {
			String[] parts = value.toString().split("::");
//			String str =parts[1] + "|"+parts[0];
			
			ArrayList<String> movies = new ArrayList<String>();
			MyMap val = new MyMap();

			val.put(new Text(parts[1]+","), new Text(parts[2]));
//			val.put(new Text("usr:"+"1"), new Text("rating:"+"1"));
//			context.write(new Text("movie:"+str), val);
			context.write(new Text(parts[0]), val);		
			
		}
	}
	public static class UserMapper extends Mapper<LongWritable, Text,Text, MovieWritable>{


			@Override
			protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text,  MovieWritable>.Context context)
					throws IOException, InterruptedException {
				String values = value.toString().substring(4, value.toString().length()-1);
				String parts[] = values.split(", ");
				MyMap val = new MyMap();
				val.put(new Text("1"), new Text("1"));
				List<String> movies = new ArrayList<String>();
				for(int i=0,n=0;n<parts.length;i=i+1) {
					movies.add(parts[n]);
					n=n+2;
				}
				MovieWritable movieWritable = new MovieWritable();
				LongArrayWritable arrayWritable = new LongArrayWritable();
				for(int m=0;m<movies.size();m++) {
					for(int d=1;d<movies.size();d++) {
						if(movies.get(m).toString()!=movies.get(d).toString()){
						String pairmovie = "("+movies.get(m).toString().split(",")[0]+","+movies.get(d).toString().split(",")[0]+")";
						movieWritable.setRating_1(new IntWritable(Integer.parseInt(movies.get(m).toString().split(",")[1])));
						movieWritable.setRating_2(new IntWritable(Integer.parseInt(movies.get(d).toString().split(",")[1])));
						movieWritable.setUser(new Text(value.toString().split("\t")[0]));
						
						context.write(new Text(pairmovie), movieWritable);
						}
						}
				}
				}
							
			
	}
	public static class MyReducer extends Reducer<Text, MyMap, Text, ArrayList>{

		@Override
		protected void reduce(Text key, Iterable<MyMap> values,
				Reducer<Text, MyMap, Text,ArrayList>.Context context) throws IOException, InterruptedException {
			
			
			MyMap value = new MyMap();
			ArrayList<String> info = new ArrayList<String>();
			//StringArrayWritable info_ar = new StringArrayWritable();
			for(MyMap a:values) {
				info.add(a.toString());
			}
			
//			for(MyMap a:values)
//				vsalue.putAll(a);
				//value.put(new Text(key.toString()),a);
			//info_ar.set(info.toArray());
			context.write(key, info);
			
		}
		
	}
	public static class MyReducer_2 extends Reducer<Text , MovieWritable, Text,  ArrayList>{

		@Override
		protected void reduce(Text key, Iterable< MovieWritable> values,
				Reducer<Text,  MovieWritable, Text,  ArrayList>.Context context) throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			MyMap value = new MyMap();
			ArrayList<String> info = new ArrayList<String>();

			for(MovieWritable a:values) {
				info.add("("+a.toString()+")");
			}

			context.write(key, info);
			
		}
		
	}
	/*public static class MyAnl extends Reducer<Text, MyMap, Text, MyMap>{

		@Override
		protected void reduce(Text arg0, Iterable<MyMap> arg1, Reducer<Text, MyMap, Text, MyMap>.Context arg2)
				throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			
		}
		
	}*/

		
		
	
	
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "Moives Digest");
		Path out = new Path(args[1]);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(MyMap.class);
		
		MultipleInputs.addInputPath(job, new Path(args[0]), TextInputFormat.class,MovieMapper.class);
		//MultipleInputs.addInputPath(job, new Path(args[0]), TextInputFormat.class,UserMapper.class);
		
		job.setReducerClass(MyReducer.class);
		//job.setReducerClass(MyAny.class);
	    
		FileOutputFormat.setOutputPath(job, new Path(out, "out1"));
	    if (!job.waitForCompletion(true)) {
	      System.exit(1);
	    }
	    Job job_2 = Job.getInstance(conf,"user-movies");
	    
	    job_2.setMapOutputKeyClass(Text.class);
	    job_2.setMapOutputValueClass(MovieWritable.class);
	    
	    
	    MultipleInputs.addInputPath(job_2, new Path(out, "out1"), TextInputFormat.class,UserMapper.class);
	    job_2.setReducerClass(MyReducer_2.class);
	    FileOutputFormat.setOutputPath(job_2, new Path(out, "out2"));
	    if (!job_2.waitForCompletion(true)) {
	        System.exit(1);
	      }
	}
}
