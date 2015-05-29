package iie.udps.example.spark;

import iie.udps.common.hcatalog.SerHCatOutputFormat;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.net.URI;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.HiveMetaStoreClient;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.SerDeInfo;
import org.apache.hadoop.hive.metastore.api.StorageDescriptor;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.ql.io.RCFileInputFormat;
import org.apache.hadoop.hive.ql.io.RCFileOutputFormat;
import org.apache.hadoop.hive.serde.serdeConstants;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.Progressable;
import org.apache.hive.hcatalog.common.HCatUtil;
import org.apache.hive.hcatalog.data.schema.HCatSchema;
import org.apache.hive.hcatalog.mapreduce.OutputJobInfo;
import org.apache.spark.SerializableWritable;
import org.apache.thrift.TException;
import org.dom4j.Document;
import org.dom4j.DocumentHelper;
import org.dom4j.Element;
import org.dom4j.io.OutputFormat;
import org.dom4j.io.XMLWriter;

/**
 * Dom4j 生成XML文档与解析XML文档
 */
public class OperatorParamXml {
	
	@SuppressWarnings("rawtypes")
	public List<Map> parseStdinXml(String stdinXml) throws Exception {

		// 读取stdin.xml文件
		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(conf);
		FSDataInputStream dis = fs.open(new Path(stdinXml));
		InputStreamReader isr = new InputStreamReader(dis, "utf-8");
		BufferedReader read = new BufferedReader(isr);
		String tempString = "";
		String xmlParams = "";
		while ((tempString = read.readLine()) != null) {
			xmlParams += "\n" + tempString;
		}
		read.close();
		xmlParams = xmlParams.substring(1);

		String userName = null;
		String operatorName = null;
		String inputDBName = null;
		String outputDBName = null;
		String inputTabName = null;
		String outputTabName = null;
		String strs = null;
		String fieldName = null;
		String inputFilePath = null;
		String schemaList = null;
		String jobinstanceid = null;
		String tempHdfsBasePath = null;
		String queueName = null;
		String processId = null;
		String jobId = null;
		String hiveServerAddress = null;
		String departmentId = null;

		List<Map> list = new ArrayList<Map>();
		Map<String, String> map = new HashMap<String, String>();
		Document document = DocumentHelper.parseText(xmlParams); // 将字符串转化为xml
		Element node1 = document.getRootElement(); // 获得根节点
		Iterator iter1 = node1.elementIterator(); // 获取根节点下的子节点
		while (iter1.hasNext()) {
			Element node2 = (Element) iter1.next();
			if ("jobinstanceid".equals(node2.getName())) {
				jobinstanceid = node2.getText();
				map.put("jobinstanceid", jobinstanceid);
				System.out.println("====jobinstanceid=====" + jobinstanceid);
			}
			// 获取通用参数
			if ("context".equals(node2.getName())) {
				Iterator iter2 = node2.elementIterator();
				while (iter2.hasNext()) {
					Element node3 = (Element) iter2.next();
					if ("property".equals(node3.getName())) {
						if ("userName".equals(node3.attributeValue("name"))) {
							userName = node3.attributeValue("value");
							map.put("userName", userName);
						} else if ("queueName".equals(node3
								.attributeValue("name"))) {
							queueName = node3.attributeValue("value");
							map.put("queueName", queueName);
						} else if ("processId".equals(node3
								.attributeValue("name"))) {
							processId = node3.attributeValue("value");
							map.put("processId", processId);
						} else if ("jobId".equals(node3.attributeValue("name"))) {
							jobId = node3.attributeValue("value");
							map.put("jobId", jobId);
						} else if ("hiveServerAddress".equals(node3
								.attributeValue("name"))) {
							hiveServerAddress = node3.attributeValue("value");
							map.put("hiveServerAddress", hiveServerAddress);
						} else if ("outputDBName".equals(node3
								.attributeValue("name"))) {
							outputDBName = node3.attributeValue("value");
							map.put("outputDBName", outputDBName);
						} else if ("tempHdfsBasePath".equals(node3
								.attributeValue("name"))) {
							tempHdfsBasePath = node3.attributeValue("value");
							map.put("tempHdfsBasePath", tempHdfsBasePath);
						} else if ("departmentId".equals(node3
								.attributeValue("name"))) {
							departmentId = node3.attributeValue("value");
							map.put("departmentId", departmentId);
						}
					}
				}
			}
			// 获取算子参数
			if ("operator".equals(node2.getName())) {
				operatorName = node2.attributeValue("name");
				map.put("operatorName", operatorName);
				Iterator iter2 = node2.elementIterator();
				while (iter2.hasNext()) {
					Element node3 = (Element) iter2.next();
					if ("parameter".equals(node3.getName())) {
						if ("field1".equals(node3.attributeValue("name"))) {
							fieldName = node3.getText();
							map.put("fieldName", fieldName);
						}
						if ("outputTableName".equals(node3.attributeValue("name"))) {
							String tempStr = node3.getText(); // 获得数据库.表格式字符串
							if (!"".equals(tempStr.trim())) {
								String[] arr = tempStr.split("\\.");
								outputDBName = arr[0];
								outputTabName = arr[1];
							}
							map.put("outputDBName", outputDBName);
							map.put("outputTabName", outputTabName);
						}
						if ("inputFilePath"
								.equals(node3.attributeValue("name"))) {
							inputFilePath = node3.getText();
							map.put("inputFilePath", inputFilePath);
						}
						if ("schemaList".equals(node3.attributeValue("name"))) {
							schemaList = node3.getText();
							map.put("schemaList", schemaList);
						}
					}
				}
			}

			// 获取输入数据库
			if ("datasets".equals(node2.getName())) {
				Iterator iter2 = node2.elementIterator();
				while (iter2.hasNext()) {
					Element node3 = (Element) iter2.next();
					if ("inport1".equals(node3.attributeValue("name"))) {
						Iterator iter3 = node3.elementIterator();
						while (iter3.hasNext()) {
							Element node4 = (Element) iter3.next();
							strs = node4.getText();
						}
						if (!"".equals(strs.trim())) {
							String[] arr = strs.split("\\.");
							if(arr.length == 2){
								inputDBName = arr[0];
								inputTabName = arr[1];
							}							
						}
						map.put("inputDBName", inputDBName);
						map.put("inputTabName", inputTabName);
					}
				}
			}
		}
		list.add(map);
		return list;
	}

	@SuppressWarnings("rawtypes")
	public List<Map> parseStdoutXml(String stdinXml) throws Exception {

		// 读取stdin.xml文件
		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(conf);
		FSDataInputStream dis = fs.open(new Path(stdinXml));
		InputStreamReader isr = new InputStreamReader(dis, "utf-8");
		BufferedReader read = new BufferedReader(isr);
		String tempString = "";
		String xmlParams = "";
		while ((tempString = read.readLine()) != null) {
			xmlParams += "\n" + tempString;
		}
		read.close();
		xmlParams = xmlParams.substring(1);

		String outputDBName = null;
		String outputTabName = null;
		String jobinstanceid = null;

		List<Map> list = new ArrayList<Map>();
		Map<String, String> map = new HashMap<String, String>();
		Document document = DocumentHelper.parseText(xmlParams); // 将字符串转化为xml
		Element node1 = document.getRootElement(); // 获得根节点
		Iterator iter1 = node1.elementIterator(); // 获取根节点下的子节点
		while (iter1.hasNext()) {
			Element node2 = (Element) iter1.next();
			if ("jobinstanceid".equals(node2.getName())) {
				jobinstanceid = node2.getText();
				map.put("jobinstanceid", jobinstanceid);
				System.out.println("====jobinstanceid=====" + jobinstanceid);
			}
			// 获取输入数据库
			if ("datasets".equals(node2.getName())) {
				Iterator iter2 = node2.elementIterator();
				while (iter2.hasNext()) {
					Element node3 = (Element) iter2.next();
					if ("outport1".equals(node3.attributeValue("name"))) {
						Iterator iter3 = node3.elementIterator();
						String strs = null;
						while (iter3.hasNext()) {
							Element node4 = (Element) iter3.next();
							strs = node4.getText();
						}
						if (!"".equals(strs.trim())) {
							String[] arr = strs.split("\\.");
							outputDBName = arr[0];
							outputTabName = arr[1];
						}
						map.put("outputDBName", outputDBName);
						map.put("outputTabName", outputTabName);
					}
				}
			}
		}
		list.add(map);
		return list;
	}

	/* 生成stdout.Xml文件 */
	@SuppressWarnings("rawtypes")
	public void genStdoutXml(String fileName, Map listOut) {

		String jobinstance = null;
		String outputDBName = null;
		String outputTabName = null;

		jobinstance = listOut.get("jobinstanceid").toString();
		outputDBName = listOut.get("outputDBName").toString();
		outputTabName = listOut.get("outputTabName").toString();

		Document document = DocumentHelper.createDocument();
		Element response = document.addElement("response");
		Element jobinstanceid = response.addElement("jobinstanceid");
		jobinstanceid.setText(jobinstance);
		Element datasets = response.addElement("datasets");
		Element dataset = datasets.addElement("dataset");
		dataset.addAttribute("name", "outport1");
		Element row = dataset.addElement("row");
		row.setText(outputDBName + "." + outputTabName);

		try {
			Configuration conf = new Configuration();
			FileSystem fs = FileSystem.get(URI.create(fileName), conf);
			OutputStream out = fs.create(new Path(fileName),
					new Progressable() {
						public void progress() {
						}
					});
			OutputFormat format = OutputFormat.createPrettyPrint();
			format.setEncoding("UTF-8");
			XMLWriter xmlWriter = new XMLWriter(out, format);
			xmlWriter.write(document);
			xmlWriter.close();
		} catch (IOException e) {
			System.out.println(e.getMessage());
		}

	}

	/* 生成stderr.xml文件 */
	@SuppressWarnings("rawtypes")
	public void genStderrXml(String fileName, Map listOut) {

		String jobinstance = null;
		String errorMessage = null;
		String errotCode = null;
		jobinstance = listOut.get("jobinstanceid").toString();
		errorMessage = listOut.get("errorMessage").toString();
		errotCode = listOut.get("errotCode").toString();

		Document document = DocumentHelper.createDocument();
		Element response = document.addElement("error");
		Element jobinstanceid = response.addElement("jobinstanceid");
		jobinstanceid.setText(jobinstance);
		Element code = response.addElement("code");
		code.setText(errotCode);
		Element message = response.addElement("message");
		message.setText(errorMessage);

		try {
			Configuration conf = new Configuration();
			FileSystem fs = FileSystem.get(URI.create(fileName), conf);
			OutputStream out = fs.create(new Path(fileName),
					new Progressable() {
						public void progress() {
						}
					});
			OutputFormat format = OutputFormat.createPrettyPrint();
			format.setEncoding("UTF-8");
			XMLWriter xmlWriter = new XMLWriter(out, format);
			xmlWriter.write(document);
			xmlWriter.close();
		} catch (IOException e) {
			System.out.println(e.getMessage());
		}
	}

	/**
	 * 获得表模式
	 * 
	 * @param dbName
	 * @param tblName
	 * @return
	 */
	public HCatSchema getHCatSchema(String dbName, String tblName) {
		Job outputJob = null;
		HCatSchema schema = null;
		try {
			outputJob = Job.getInstance();
			outputJob.setJobName("getHCatSchema");
			outputJob.setOutputFormatClass(SerHCatOutputFormat.class);
			outputJob.setOutputKeyClass(WritableComparable.class);
			outputJob.setOutputValueClass(SerializableWritable.class);
			SerHCatOutputFormat.setOutput(outputJob,
					OutputJobInfo.create(dbName, tblName, null));
			schema = SerHCatOutputFormat.getTableSchema(outputJob
					.getConfiguration());
		} catch (IOException e) {
			e.printStackTrace();
		}
		return schema;
	}

	/**
	 * 指定数据库、表名，创建表
	 * 
	 * @param dbName
	 * @param tblName
	 * @param schema
	 */
	public void createTable(String dbName, String tblName, HCatSchema schema) {
		HiveMetaStoreClient client = null;
		try {
			HiveConf hiveConf = HCatUtil.getHiveConf(new Configuration());
			client = HCatUtil.getHiveClient(hiveConf);
		} catch (MetaException | IOException e) {
			e.printStackTrace();
		}
		try {
			if (client.tableExists(dbName, tblName)) {
				client.dropTable(dbName, tblName);
			}
		} catch (TException e) {
			e.printStackTrace();
		}
		// 获取表模式
		List<FieldSchema> fields = HCatUtil.getFieldSchemaList(schema
				.getFields());
		// 生成表对象
		Table table = new Table();
		table.setDbName(dbName);
		table.setTableName(tblName);

		StorageDescriptor sd = new StorageDescriptor();
		sd.setCols(fields);
		table.setSd(sd);
		sd.setInputFormat(RCFileInputFormat.class.getName());
		sd.setOutputFormat(RCFileOutputFormat.class.getName());
		sd.setParameters(new HashMap<String, String>());
		sd.setSerdeInfo(new SerDeInfo());
		sd.getSerdeInfo().setName(table.getTableName());
		sd.getSerdeInfo().setParameters(new HashMap<String, String>());
		sd.getSerdeInfo().getParameters()
				.put(serdeConstants.SERIALIZATION_FORMAT, "1");
		sd.getSerdeInfo().setSerializationLib(
				org.apache.hadoop.hive.serde2.columnar.ColumnarSerDe.class
						.getName());
		Map<String, String> tableParams = new HashMap<String, String>();
		table.setParameters(tableParams);
		try {
			client.createTable(table);
			System.out.println("Create table successfully!");
		} catch (TException e) {
			e.printStackTrace();
			return;
		} finally {
			client.close();
		}
	}
	
	/**
     * 以行为单位读取文件，常用于读面向行的格式化文件
     */
    public static void readFileByLines(String fileName) {
        File file = new File(fileName);
        BufferedReader reader = null;
        try {
            System.out.println("以行为单位读取文件内容，一次读一整行：");
            reader = new BufferedReader(new FileReader(file));
            String tempString = null;
            int line = 1;
            // 一次读入一行，直到读入null为文件结束
            while ((tempString = reader.readLine()) != null) {
                // 显示行号
                System.out.println("line " + line + ": " + tempString);
                line++;
            }
            reader.close();
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            if (reader != null) {
                try {
                    reader.close();
                } catch (IOException e1) {
                }
            }
        }
    }

}