package iie.udps.example.java;

import iie.udps.example.spark.OperatorParamXml;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.hive.hcatalog.common.HCatException;
import org.apache.hive.hcatalog.data.DefaultHCatRecord;
import org.apache.hive.hcatalog.data.HCatRecord;
import org.apache.hive.hcatalog.data.schema.HCatSchema;
import org.apache.hive.hcatalog.data.transfer.DataTransferFactory;
import org.apache.hive.hcatalog.data.transfer.HCatReader;
import org.apache.hive.hcatalog.data.transfer.HCatWriter;
import org.apache.hive.hcatalog.data.transfer.ReadEntity;
import org.apache.hive.hcatalog.data.transfer.ReaderContext;
import org.apache.hive.hcatalog.data.transfer.WriteEntity;
import org.apache.hive.hcatalog.data.transfer.WriterContext;

public class JavaCountByAge {

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
			// 从hive表中读出数据
			ReaderContext context = getContext(inputDBName, inputTabName);

			// 根据输入表结构，创建与输入表同样结构的输出表
			HCatSchema schema = operXML
					.getHCatSchema(inputDBName, inputTabName);
			// operXML.createTable(outputDBName, outputTabName, schema);

			// 将指定列的数据大写，并将表数据存回输出表
			storeToTable(outputDBName, outputTabName, context, schema);

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
	 * 将处理后的数据存到输出表中
	 * 
	 * @param dbName
	 * @param featureTable
	 * @throws HCatException
	 */
	public static void storeToTable(String dbName, String featureTable,
			ReaderContext data, HCatSchema schema) throws HCatException {
		List<HCatRecord> records = new ArrayList<HCatRecord>();
		// 读取数据
		Map<Integer, Integer> hashMap = new HashMap<Integer, Integer>();
		List<Integer> word_list = new ArrayList<Integer>();
		Integer age = null;
		Integer count = null;
		// 将读取到的数据内容，处理后封装成HCatRecord对象，用write接口写会hive表
		for (int i = 0; i < data.numSplits(); i++) {
			HCatReader splitReader = DataTransferFactory.getHCatReader(data, i);
			Iterator<HCatRecord> itr1 = splitReader.read();
			while (itr1.hasNext()) {
				HCatRecord record = itr1.next();
				age = record.getInteger("age", schema);
				word_list.add(age);
			}
		}
		for (Integer temp : word_list) {
			count = hashMap.get(temp);
			hashMap.put(temp, (count == null) ? 1 : count + 1);
		}

		// 创建一个WriteEntity 指定数据写到哪里
		WriteEntity.Builder builder = new WriteEntity.Builder();
		WriteEntity entity = builder.withDatabase(dbName)
				.withTable(featureTable).build();
		// 创建WriterContext
		Map<String, String> config = new HashMap<String, String>();
		HCatWriter writer = DataTransferFactory.getHCatWriter(entity, config);
		WriterContext context = writer.prepareWrite();
		HCatWriter splitWriter = DataTransferFactory.getHCatWriter(context);

		// 用HCatWriter接口写数据
		Set<Integer> keys = hashMap.keySet();
		for (Integer key : keys) {
			HCatRecord record = new DefaultHCatRecord(2);
			record.set(0, key);
			record.set(1, hashMap.get(key));
			records.add(record);
		}
		splitWriter.write(records.iterator());
		writer.commit(context);
	}

	/**
	 * 读取hive表中数据
	 * 
	 * @param dbName
	 * @param inputTable
	 * @return
	 * @throws HCatException
	 */
	public static ReaderContext getContext(String dbName, String inputTable)
			throws HCatException {
		ReadEntity.Builder builder = new ReadEntity.Builder();
		ReadEntity entity = builder.withDatabase(dbName).withTable(inputTable)
				.build();
		Map<String, String> config = new HashMap<String, String>();
		HCatReader reader = DataTransferFactory.getHCatReader(entity, config);
		ReaderContext context = reader.prepareRead();
		return context;
	}
}
