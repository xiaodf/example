package iie.udps.example.spark;

import iie.udps.common.hcatalog.SerHCatInputFormat;
import iie.udps.common.hcatalog.SerHCatOutputFormat;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hive.hcatalog.data.DefaultHCatRecord;
import org.apache.hive.hcatalog.data.HCatRecord;
import org.apache.hive.hcatalog.data.schema.HCatSchema;
import org.apache.hive.hcatalog.mapreduce.OutputJobInfo;
import org.apache.spark.SerializableWritable;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.Function2;

import scala.Tuple2;

/**
 * 实现功能：spark+hcatlog 读hive表数据，将其中一列数据大写后写会另一张hive表中
 *
 */
public class SparkCountByAge {

	@SuppressWarnings("rawtypes")
	public static void main(String[] args) throws Exception {
		if (args.length < 2) {
			System.err.println("Usage: <-c> <stdin.xml>");
			System.exit(1);
		}
		
		String stdinXml = args[1];
		OperatorParamXml operXML = new OperatorParamXml();
		List<java.util.Map> stdinList = operXML.parseStdinXml(stdinXml);// 参数列表

		// 获得输入参数
		String inputDBName = stdinList.get(0).get("inputDBName").toString();
		String inputTabName = stdinList.get(0).get("inputTabName").toString();
		String outputDBName = stdinList.get(0).get("outputDBName").toString();
		String outputTabName = stdinList.get(0).get("outputTabName").toString();
		String tempHdfsBasePath = stdinList.get(0).get("tempHdfsBasePath")
				.toString();
		String jobinstanceid = stdinList.get(0).get("jobinstanceid").toString();

		if (inputDBName == "" || inputTabName == "" || jobinstanceid == ""
				|| outputDBName == "" || outputTabName == ""
				|| tempHdfsBasePath == "" || jobinstanceid == "") {

			// 设置异常输出参数
			java.util.Map<String, String> stderrMap = new HashMap<String, String>();
			String errorMessage = "Some operating parameters is empty!!!";
			String errotCode = "80001";
			stderrMap.put("errorMessage", errorMessage);
			stderrMap.put("errotCode", errotCode);
			stderrMap.put("jobinstanceid", jobinstanceid);
			String fileName = "";
			if (tempHdfsBasePath.endsWith("/")) {
				fileName = tempHdfsBasePath + "stderr.xml";
			} else {
				fileName = tempHdfsBasePath + "/stderr.xml";
			}
			
			// 生成异常输出文件
			operXML.genStderrXml(fileName, stderrMap);
		} else {			
			// 根据输入表结构，创建与输入表同样结构的输出表
			HCatSchema schema = operXML
					.getHCatSchema(inputDBName, inputTabName);

			// Spark程序第一件事情就是创建一个JavaSparkContext告诉Spark怎么连接集群
			SparkConf sparkConf = new SparkConf().setAppName("SparkExample");
			JavaSparkContext jsc = new JavaSparkContext(sparkConf);
			
			// 读取并处理hive表中的数据，生成RDD数据并处理后返回
			JavaRDD<SerializableWritable<HCatRecord>> LastRDD = getProcessedData(
					jsc, inputDBName, inputTabName, schema);
			
			// 将处理后的数据存到hive输出表中
			storeToTable(LastRDD, outputDBName, outputTabName);

			jsc.stop();

			// 设置正常输出参数
			java.util.Map<String, String> stdoutMap = new HashMap<String, String>();
			stdoutMap.put("outputDBName", outputDBName);
			stdoutMap.put("outputTabName", outputTabName);
			stdoutMap.put("jobinstanceid", jobinstanceid);
			String fileName = "";
			if (tempHdfsBasePath.endsWith("/")) {
				fileName = tempHdfsBasePath + "stdout.xml";
			} else {
				fileName = tempHdfsBasePath + "/stdout.xml";
			}
			
			// 生成正常输出文件
			operXML.genStdoutXml(fileName, stdoutMap);
		}
		System.exit(0);
	}

	/**
	 * 
	 * @param jsc
	 * @param dbName
	 * @param inputTable
	 * @param fieldPosition
	 * @return
	 * @throws IOException
	 */
	@SuppressWarnings("rawtypes")
	public static JavaRDD<SerializableWritable<HCatRecord>> getProcessedData(
			JavaSparkContext jsc, String dbName, String inputTable,
			final HCatSchema schema) throws IOException {
		// 获取hive表数据
		Configuration inputConf = new Configuration();
		Job job = Job.getInstance(inputConf);
		SerHCatInputFormat.setInput(job.getConfiguration(), dbName, inputTable);
		JavaPairRDD<WritableComparable, SerializableWritable> rdd = jsc
				.newAPIHadoopRDD(job.getConfiguration(),
						SerHCatInputFormat.class, WritableComparable.class,
						SerializableWritable.class);

		// 获取表记录集
		JavaPairRDD<Integer, Integer> pairs = rdd
				.mapToPair(new PairFunction<Tuple2<WritableComparable, SerializableWritable>, Integer, Integer>() {
					private static final long serialVersionUID = 1L;

					@SuppressWarnings("unchecked")
					@Override
					public Tuple2<Integer, Integer> call(
							Tuple2<WritableComparable, SerializableWritable> value)
							throws Exception {
						HCatRecord record = (HCatRecord) value._2.value();
						return new Tuple2((Integer) record.get(1), 1);
					}
				});

		JavaPairRDD<Integer, Integer> counts = pairs
				.reduceByKey(new Function2<Integer, Integer, Integer>() {
					private static final long serialVersionUID = 1L;

					@Override
					public Integer call(Integer i1, Integer i2) {
						return i1 + i2;
					}
				});

		JavaRDD<SerializableWritable<HCatRecord>> messageRDD = counts
				.map(new Function<Tuple2<Integer, Integer>, SerializableWritable<HCatRecord>>() {
					private static final long serialVersionUID = 1L;

					@Override
					public SerializableWritable<HCatRecord> call(
							Tuple2<Integer, Integer> arg0) throws Exception {
						HCatRecord record = new DefaultHCatRecord(2);
						record.set(0, arg0._1);
						record.set(1, arg0._2);
						return new SerializableWritable<HCatRecord>(record);
					}
				});
		// 返回处理后的数据
		return messageRDD;
	}

	/**
	 * 将处理后的数据存到输出表中
	 * 
	 * @param rdd
	 * @param dbName
	 * @param tblName
	 */
	@SuppressWarnings("rawtypes")
	public static void storeToTable(
			JavaRDD<SerializableWritable<HCatRecord>> rdd, String dbName,
			String tblName) {
		Job outputJob = null;
		try {
			outputJob = Job.getInstance();
			outputJob.setJobName("SparkExample");
			outputJob.setOutputFormatClass(SerHCatOutputFormat.class);
			outputJob.setOutputKeyClass(WritableComparable.class);
			outputJob.setOutputValueClass(SerializableWritable.class);
			SerHCatOutputFormat.setOutput(outputJob,
					OutputJobInfo.create(dbName, tblName, null));
			HCatSchema schema = SerHCatOutputFormat
					.getTableSchemaWithPart(outputJob.getConfiguration());
			SerHCatOutputFormat.setSchema(outputJob, schema);
		} catch (IOException e) {
			e.printStackTrace();
		}

		// 将RDD存储到目标表中
		rdd.mapToPair(
				new PairFunction<SerializableWritable<HCatRecord>, WritableComparable, SerializableWritable<HCatRecord>>() {
					private static final long serialVersionUID = -4658431554556766962L;

					public Tuple2<WritableComparable, SerializableWritable<HCatRecord>> call(
							SerializableWritable<HCatRecord> record)
							throws Exception {
						return new Tuple2<WritableComparable, SerializableWritable<HCatRecord>>(
								NullWritable.get(), record);
					}
				}).saveAsNewAPIHadoopDataset(outputJob.getConfiguration());

	}

}
