package iie.udps.example.spark;

import java.io.BufferedReader;
import java.io.FileReader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import org.dom4j.Document;
import org.dom4j.DocumentHelper;
import org.dom4j.Element;

public class test {

	@SuppressWarnings("rawtypes")
	public static void main(String[] args) throws Exception {
		
		String file = "f:/TestStdin.xml";
    
        FileReader reader = new FileReader(file);
        BufferedReader br = new BufferedReader(reader);
        String str = "";   
        String xmlParams = "";
        while((str = br.readLine()) != null) {      
              xmlParams += str + "\n";
        }       
        reader.close();
		
		List<Map> stdinList = parseStdinXml(xmlParams);// 参数列表

		// 获得输入参数
//		String inputDBName = stdinList.get(0).get("inputDBName").toString();
//		String inputTabName = stdinList.get(0).get("inputTabName").toString();
		String fieldName = stdinList.get(0).get("fieldName").toString();// 要转换成大写的字段
		//String outputDBName = stdinList.get(0).get("outputDBName").toString();
		//String outputTabName = stdinList.get(0).get("outputTabName").toString();
//		String tempHdfsBasePath = stdinList.get(0).get("tempHdfsBasePath")
//				.toString();
//		String jobinstanceid = stdinList.get(0).get("jobinstanceid").toString();
		//System.out.println(stdinList.get(0).containsKey("outputDBName"));
		System.out.println(fieldName);
		//System.out.println(outputDBName+": "+outputTabName);
	}
	
	@SuppressWarnings("rawtypes")
	public static List<Map> parseStdinXml(String stdinXml) throws Exception {

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
		Document document = DocumentHelper.parseText(stdinXml); // 将字符串转化为xml
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
								if(arr.length==2){
									outputDBName = arr[0];
									outputTabName = arr[1];	
								}								
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
							inputDBName = arr[0];
							inputTabName = arr[1];
						}
						map.put("inputDBName", inputDBName);
						map.put("inputTabName", inputTabName);
					}

					// if ("outport1".equals(node3.attributeValue("name"))) {
					// Iterator iter3 = node3.elementIterator();
					// while (iter3.hasNext()) {
					// Element node4 = (Element) iter3.next();
					// strs = node4.getText();
					// }
					// if (!"".equals(strs.trim())) {
					// String[] arr = strs.split("\\.");
					// outputDBName = arr[0];
					// outputTabName = arr[1];
					// }
					// map.put("outputDBName", outputDBName);
					// map.put("outputTabName", outputTabName);
					// }
				}
			}
		}
		list.add(map);
		return list;
	}

}
