package sp;

import java.io.Serializable;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;

import java.util.Iterator;




import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.Optional;
import org.apache.spark.api.java.function.FlatMapFunction;

import org.apache.spark.api.java.function.PairFunction;

import scala.Tuple2;

import scala.collection.mutable.StringBuilder;

//Firstly,
//I find out the the destination node as the key and their following path, therefore my step1 result will like this
//key----value
//N1 [(N3,4),(N5,7)] 
//N3 [(N4,4),(N4,7)]
//
//Then,
//there are some node in the map ending, and didn't store in step1 RDD.
//Therefore, I add a step to find the node and left-join with the step, meanwhile this step will initial all node distance with starting node
//as Integet.Max_value except starting node.
//Next, i will use map reduce to update each node distance 
//if their current distance is bigger than updated distance, it would be change to the new one.
//After (Vertx number-1) times will find out shortest path, and there are no updating anymore.
//Each time function should  scan  all node and find the shortest path, the String value (Path = "Staringnode -N1-N3-N4")will be append to RDD 
//For example
//Staring node: N3
//(destination, distance) (path)  (Adjacency_Node)
//(N1,4) N3-N1 [(N3,4),(N5,7)]
//From far to near ， so class IntComparator will help me to sort the RDD
//Finally, use a map function to write (destination, distance) (path) as result 
//
//However, I‘m getting stuck on updating step.
//


public class AssigTwoz5127584 {

	public static class Node implements Serializable{
		Integer distance;
		String path;
		public Node(Integer distance, String path) {
			super();
			this.distance = distance;
			this.path = path;
		}
		public String toString() {
			StringBuilder sb =new StringBuilder();
			sb.append(distance).append(",").append(path);
			return sb.toString();
		}
	}
//	public static class Node implements Serializable{
//		/**
//		 * 
//		 */
//		private static final long serialVersionUID = 1L;
//		private String distance;
//		private String[] adjs;
//		public String getDistance() {
//			return distance;
//		}
//		
//		public void setDistance(String distance) {
//			this.distance = distance;
//		}
//		
//		public String getKey(String str) {
//			return str.substring(1, str.indexOf(","));
//		}
//		
//		public String getValue(String str) {
//			return str.substring(str.indexOf(",")+1,str.indexOf(""));
//		}
//		
//		public String getNodeKey(int num) {
//			return getKey(adjs[num]);
//		}
//		
//		public String getNodeValue(int num) {
//			return getValue(adjs[num]);
//		}
//		
//		public int getNodeNum() {
//			return adjs.length;
//		}
//		
//		public void FormatNode(String str) {
//			if(str.length() == 0)
//				return;
//			String []strs = StringUtils.split(str,'\t');
//			
//			adjs = new String[strs.length-1];
//			for(int i=0;i<strs.length;i++) {
//				if(i == 0) {
//					setDistance(strs[i]);
//					continue;
//				}
//				this.adjs[i-1] = strs[i];
//			}
//		}
//		
//		public String toString() {
//			String str = this.distance+"";
//			if(this.adjs == null)
//				return str;
//			for(String s:this.adjs) {
//				str = str+"\t"+s;
//			}
//			return str;
//		}
//	}

	public static class IntComparator implements Comparator<Tuple2<String, Integer>>,Serializable{

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	@Override
	public int compare(Tuple2<String, Integer> o1, Tuple2<String, Integer> o2) {
		// TODO Auto-generated method stub
		return o1._2-o2._2;
	}
	}
	
	
	
	public static void main(String[] args) {
		// TODO Auto-generated method stub
		SparkConf conf = new SparkConf()
				.setAppName("Short Path")
				.setMaster("local");
		
		JavaSparkContext context = new JavaSparkContext(conf);
		ArrayList<String> node = new ArrayList<String>();
		ArrayList<String> list = new ArrayList<String>();
		JavaRDD<String> input = context.textFile(args[1]);
		String Start_point = args[0];
		JavaPairRDD<Tuple2<String, Integer>,Tuple2<String, Integer>> step1 = input.mapToPair(new PairFunction<String,Tuple2<String, Integer>,Tuple2<String, Integer>>() {

			@Override
			public Tuple2<Tuple2<String, Integer>, Tuple2<String, Integer>> call(String line) throws Exception {
				String[] parts = line.split(",");
				String source_vertx1 = parts[0];
				String vertx2 = parts[1];
				Integer dis;
				Integer weights = Integer.parseInt(parts[2]);
				if(source_vertx1.equals(Start_point)){
					dis = 0;
				}else {
					dis= Integer.MAX_VALUE;
				}
				return new Tuple2<Tuple2<String,Integer>, Tuple2<String,Integer>>(new Tuple2<>(source_vertx1,dis), new Tuple2<>(vertx2, weights));
			}
		});
		JavaPairRDD<String, Iterable<Integer>>step2 = context.textFile(args[1])
		.flatMap(sentence -> Arrays.asList(sentence.split(",")).iterator())
		.mapToPair(t -> new Tuple2<>(t, 1))
		.groupByKey();
		step2.keys().collect().forEach(node::add);
		
		for(int i =0;i<node.size();i++) {
			if(node.get(i).charAt(0)=='N') {
				list.add(node.get(i));
			}else {
				continue;
			}
		}
		
		JavaPairRDD<String, Integer>step3 = input.mapToPair(new PairFunction<String, String, Integer>() {

			@Override
			public Tuple2<String, Integer> call(String line) throws Exception {
				String[] parts = line.split(",");
				String source_vertx1 = parts[0];
				String vertx2 = parts[1];
				Integer dis;
				if(source_vertx1.equals(Start_point)) {
					dis=0;
				}else {
					dis=Integer.MAX_VALUE;
				}
				return new Tuple2<String,Integer>(vertx2,dis);
			}
		});
		JavaPairRDD<String, Integer>step4 = input.mapToPair(new PairFunction<String, String, Integer>() {

			@Override
			public Tuple2<String, Integer> call(String line) throws Exception {
				String[] parts = line.split(",");
				String source_vertx1 = parts[0];
				String vertx2 = parts[1];
				Integer dis;
				if(source_vertx1.equals(Start_point)) {
					dis=0;
				}else {
					dis=Integer.MAX_VALUE;
				}
				return new Tuple2<String,Integer>(source_vertx1,dis);
			}
		});
		JavaPairRDD<String, Integer>graph = step3.distinct().union(step4.distinct());

		JavaPairRDD<String, Integer>asd = graph.mapToPair(new PairFunction<Tuple2<String,Integer>, String,Integer>() {

			@Override
			public Tuple2<String, Integer> call(Tuple2<String, Integer> line) throws Exception {
				String key = line._1;
				Integer val = 0;
				if(!key.equals(Start_point)) {
					val = Integer.MAX_VALUE;
				}
				return new Tuple2<String, Integer>(key,val);
			}
		});
		JavaPairRDD<String,Tuple2<String, Integer>> step5 = input.mapToPair(new PairFunction<String,String,Tuple2<String, Integer>>() {

			@Override
			public Tuple2<String, Tuple2<String, Integer>> call(String line) throws Exception {
				String[] parts = line.split(",");
				String source_vertx1 = parts[0];
				String vertx2 = parts[1];
				Integer dis;
				Integer weights = Integer.parseInt(parts[2]);
				
				return new Tuple2<String, Tuple2<String,Integer>>(source_vertx1, new Tuple2<>(vertx2, weights));
			}
		});
		
		//asd.distinct().collect().forEach(System.out::println);
		//step5.distinct().collect().forEach(System.out::println);
//		asd
//		.distinct()
//		.leftOuterJoin(step5.groupByKey())
//		.groupByKey().collect().forEach(System.out::println);
		JavaPairRDD<String, Iterable<Tuple2<Integer, Optional<Iterable<Tuple2<String, Integer>>>>>> sda = 
				asd
				.distinct()
				.leftOuterJoin(step5.groupByKey())
				.groupByKey();
		 JavaPairRDD<Tuple2<String,Integer>, Tuple2<String,ArrayList>>step7 = sda.mapToPair(new PairFunction<Tuple2<String,
				Iterable<Tuple2<Integer,Optional<Iterable<Tuple2<String,Integer>>>>>>, 
				Tuple2<String,Integer>, Tuple2<String,ArrayList>>() {

			@Override
			public Tuple2<Tuple2<String, Integer>, Tuple2<String, ArrayList>> call(
					Tuple2<String, Iterable<Tuple2<Integer, Optional<Iterable<Tuple2<String, Integer>>>>>> input)
					throws Exception {
				String key = input._1;
				Integer distance = null;
				String way ="";
				ArrayList<Tuple2<Integer, Optional<Iterable<Tuple2<String, Integer>>>>> info =new ArrayList<Tuple2<Integer,Optional<Iterable<Tuple2<String,Integer>>>>>();
				ArrayList<Tuple2<String,Integer>> road = new ArrayList<Tuple2<String,Integer>>();
				input._2.forEach(info::add);
				distance = info.get(0)._1;
				if(info.get(0)._2.isPresent()) {
				info.get(0)._2.get().forEach(road::add);
				}
					
				return new Tuple2<Tuple2<String, Integer>, Tuple2<String, ArrayList>>(new Tuple2<>(key,distance),new Tuple2<>(way,road));
			}
		});
		//step7.collect().forEach(System.out::println);
//		JavaRDD<Tuple2<String, Integer>> result= step7.flatMap(new FlatMapFunction<Tuple2<Tuple2<String,Integer>,Tuple2<String,ArrayList>>, Tuple2<String, Integer>>() {
//
//			@Override
//			public Iterator<Tuple2<String, Integer>> call(
//					Tuple2<Tuple2<String, Integer>, Tuple2<String, ArrayList>> input) throws Exception {
//				ArrayList<Tuple2<Tuple2<String, Integer>, Tuple2<String, ArrayList>>> info = new ArrayList<Tuple2<Tuple2<String,Integer>,Tuple2<String,ArrayList>>>();
//				String key =input._1._1;
//				Integer weight = input._1._2;
//				String path = input._2._1;
//				ArrayList<Tuple2<String, Integer>> ii= new ArrayList<Tuple2<String,Integer>>();
//				ArrayList<Tuple2<String, Integer>> i2= new ArrayList<Tuple2<String,Integer>>();
//				ii = input._2._2;
//				info.add(new Tuple2<>(new Tuple2<>(key,weight),new Tuple2<>(path,ii)));
//				
//				String te = info.get(0)._1._1;
//				Integer te1 = info.get(0)._1._2;
//				i2.add(new Tuple2<>(te,te1));
//				return i2.iterator();
//			}
//		});
		//result.collect().forEach(System.out::println);
		
		
		JavaPairRDD<Tuple2<String,Integer>, Tuple2<String,ArrayList>>result = step7.sortByKey(new IntComparator());
		java.util.List<Tuple2<Tuple2<String, Integer>, Tuple2<String, ArrayList>>> process = new ArrayList();
		result.flatMap(new FlatMapFunction<Tuple2<Tuple2<String,Integer>,Tuple2<String,ArrayList>>, String>() {

			@Override
			public Iterator<String> call(Tuple2<Tuple2<String, Integer>, Tuple2<String, ArrayList>> line)
					throws Exception {
				String ret= "";
				ret = ret + line._1._1+','+line._1._2.toString()+'.'+""+'|'+line._2._2.toString().replace("[", "").replace("]", "")+";";
				return Arrays.asList(ret).iterator();
			}
		}).saveAsTextFile("out");
//		JavaRDD<String> input_g = context.textFile("out/part-00000");
//		JavaRDD<String>a = input_g.flatMap(new FlatMapFunction<String, String>() {
//
//			@Override
//			public Iterator<String> call(String line) throws Exception {
//				String[] parts=line.split(";");
//				ArrayList<String> node= new ArrayList<String>();
//				ArrayList<Integer> distance = new ArrayList<Integer>();
//				ArrayList<String> path = new ArrayList<String>();
//				ArrayList<String> info = new ArrayList<String>();
////				for(String i:parts) {
////					node.add(i.split(".")[0].split(",")[0]);
////					distance.add(Integer.parseInt(i.split(".")[0].split(",")[1]));
////					path.add(i.split(".")[1].split("|")[0]);
////					info.add(i.split(".")[1].split("|")[1]);
////				}
//				step7.map(x->x._1._2());
//				return Arrays.asList(parts).iterator();
//				
//			}
//		});

		//process = result.collect();
		//System.out.print(process.get(3));
//		ArrayList<Tuple2<String,Integer>> node1 = new ArrayList<Tuple2<String,Integer>>();
//		node1.add(new Tuple2<>("N0",4));
//		Tuple2<Tuple2<String, Integer>, Tuple2<String, ArrayList>> elem = new Tuple2<>(new Tuple2<>("N4",2),new Tuple2<>("",node1));
//		process.add(elem);
//		System.out.print(process.get(4));
//		int flag=0;
//		while(flag<list.size()) {
//		Integer flag=process.size();
//		for(int i=0;i<flag-1;i++) {
//			String k1 = process.get(i)._1._1;
//			Integer v1 = process.get(i)._1._2;
//			ArrayList<Tuple2<String, Integer>> target1 = new ArrayList<Tuple2<String,Integer>>();
//			String path1 = process.get(i)._2._1;
//			target1=process.get(i)._2._2;
//			for(int j=i+1;j<flag;j++) {
//				String k2 = process.get(j)._1._1;
//				Integer v2 = process.get(j)._1._2;
//				ArrayList<Tuple2<String, Integer>> target2 = new ArrayList<Tuple2<String,Integer>>();
//				String path2 = process.get(j)._2._1;
//				target2=process.get(j)._2._2;
//				if(!v1.equals(Integer.MAX_VALUE)||!target1.isEmpty()) {
//					for(Tuple2<String, Integer> d:target1) {
//						if(d._1.equals(k2)) {
//							if(d._2+v1<v2) {
//								
//								process.set(j, new Tuple2<>(new Tuple2<String,Integer>(k2,d._2+v1),new Tuple2<String,ArrayList>("-"+k2,target2)));
//							}else {
//								continue;
//							}
//							System.out.print("1111");
//						}else {
//							continue;
//						}
//					}
//					
//				}else {
//					continue;
//				}
//			}
//		}
		
//		flag++;
//		}
		//process.forEach(System.out::println);
		//step7.collect().forEach(System.out::println);
		
	}

}
