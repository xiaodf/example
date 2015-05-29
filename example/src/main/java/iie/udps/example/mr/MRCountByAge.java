package iie.udps.example.mr;

import org.apache.hadoop.io.IntWritable;
import iie.udps.example.spark.OperatorParamXml;
import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hive.hcatalog.data.DefaultHCatRecord;
import org.apache.hive.hcatalog.data.HCatRecord;
import org.apache.hive.hcatalog.data.schema.HCatSchema;
import org.apache.hive.hcatalog.mapreduce.HCatInputFormat;
import org.apache.hive.hcatalog.mapreduce.HCatOutputFormat;
import org.apache.hive.hcatalog.mapreduce.OutputJobInfo;

public class MRCountByAge extends Configured implements Tool {

	@SuppressWarnings("rawtypes")
	public static class Map extends
			Mapper<WritableComparable, HCatRecord, IntWritable, IntWritable> {

		int age;
		IntWritable one = new IntWritable(1);

		@Override
		protected void map(
				WritableComparable key,
				HCatRecord value,
				org.apache.hadoop.mapreduce.Mapper<WritableComparable, HCatRecord, IntWritable, IntWritable>.Context context)
				throws IOException, InterruptedException {
			age = (Integer) value.get(1);
			context.write(new IntWritable(age), one);
		}
	}

	@SuppressWarnings("rawtypes")
	public static class Reduce extends
			Reducer<IntWritable, IntWritable, WritableComparable, HCatRecord> {
		@Override
		protected void reduce(
				IntWritable key,
				java.lang.Iterable<IntWritable> values,
				org.apache.hadoop.mapreduce.Reducer<IntWritable, IntWritable, WritableComparable, HCatRecord>.Context context)
				throws IOException, InterruptedException {
			int sum = 0;
			Iterator<IntWritable> iter = values.iterator();
			while (iter.hasNext()) {
				sum++;
				iter.next();
			}

			HCatRecord record = new DefaultHCatRecord(2);
			record.set(0, key.get());
			record.set(1, sum);
			context.write(null, record);
		}
	}

	@SuppressWarnings("rawtypes")
	public int run(String[] args) throws Exception {
		if (args.length < 2) {
			System.err.println("Usage: <-c> <stdin.xml>");
			System.exit(1);
		}

		Configuration conf = getConf();
		args = new GenericOptionsParser(conf, args).getRemainingArgs();
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
			return 0;
		} else {
			Job job = Job.getInstance(conf);
			HCatInputFormat.setInput(job.getConfiguration(), inputDBName,
					inputTabName);
			job.setInputFormatClass(HCatInputFormat.class);
			job.setJarByClass(MRCountByAge.class);
			job.setMapperClass(Map.class);
			job.setReducerClass(Reduce.class);
			job.setMapOutputKeyClass(IntWritable.class);
			job.setMapOutputValueClass(IntWritable.class);
			job.setOutputKeyClass(WritableComparable.class);
			job.setOutputValueClass(DefaultHCatRecord.class);
			HCatOutputFormat.setOutput(job,
					OutputJobInfo.create(outputDBName, outputTabName, null));
			HCatSchema s = HCatOutputFormat.getTableSchema(job
					.getConfiguration());
			System.err
					.println("INFO: output schema explicitly set for writing:"
							+ s);
			HCatOutputFormat.setSchema(job, s);
			job.setOutputFormatClass(HCatOutputFormat.class);
			int flag = job.waitForCompletion(true) ? 0 : 1;

			// 判断job是否成功执行，若成功生成正常输出xml文件，否则输出异常xml
			if (flag == 1) {
				String fileName = "";

				// 设置正常输出参数
				java.util.Map<String, String> stdoutMap = new HashMap<String, String>();
				stdoutMap.put("outputDBName", outputDBName);
				stdoutMap.put("outputTabName", outputTabName);
				stdoutMap.put("jobinstanceid", jobinstanceid);

				if (tempHdfsBasePath.endsWith("/")) {
					fileName = tempHdfsBasePath + "stdout.xml";
				} else {
					fileName = tempHdfsBasePath + "/stdout.xml";
				}

				// 生成正常输出文件
				operXML.genStdoutXml(fileName, stdoutMap);
			} else if (flag == 0) {
				String fileName = "";

				// 设置异常输出参数
				java.util.Map<String, String> stderrMap = new HashMap<String, String>();
				String errorMessage = "job is failed!!!";
				String errotCode = "80002";
				stderrMap.put("errorMessage", errorMessage);
				stderrMap.put("errotCode", errotCode);
				stderrMap.put("jobinstanceid", jobinstanceid);

				if (tempHdfsBasePath.endsWith("/")) {
					fileName = tempHdfsBasePath + "stderr.xml";
				} else {
					fileName = tempHdfsBasePath + "/stderr.xml";
				}

				// 生成异常输出文件
				operXML.genStderrXml(fileName, stderrMap);
			}
			return flag;
		}
	}

	public static void main(String[] args) throws Exception {
		int exitCode = ToolRunner.run(new MRCountByAge(), args);
		System.exit(exitCode);
	}
}