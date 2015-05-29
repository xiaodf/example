package iie.udps.example.spark;

import java.io.IOException;

import iie.udps.common.hcatalog.SerHCatInputFormat;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.spark.SerializableWritable;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;

public class spark {

	public static void main(String[] args) throws IOException {

		// Spark程序第一件事情就是创建一个JavaSparkContext告诉Spark怎么连接集群
		SparkConf sparkConf = new SparkConf().setAppName("SparkExample");
		JavaSparkContext jsc = new JavaSparkContext(sparkConf);
//		Configuration inputConf = new Configuration();
//		Job job = Job.getInstance(inputConf);
//		SerHCatInputFormat.setInput(job.getConfiguration(), "xdf", "test_out");
//		JavaPairRDD<WritableComparable, SerializableWritable> rdd = jsc
//				.newAPIHadoopRDD(job.getConfiguration(),
//						SerHCatInputFormat.class, WritableComparable.class,
//						SerializableWritable.class);
		
		for (int i = 0; i < 10000; i++) {
			System.out.println("111111111111111111");
		}
		jsc.stop();
		System.exit(0);;
	}
}
