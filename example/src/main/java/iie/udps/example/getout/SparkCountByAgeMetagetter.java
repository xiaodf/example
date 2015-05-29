package iie.udps.example.getout;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.cncert.bdap.ifc.operator.DataType;
import org.cncert.bdap.ifc.operator.FieldMetadata;
import org.cncert.bdap.ifc.operator.OutputMetadataGetter;
import org.cncert.bdap.ifc.operator.PortMetadata;
import org.dom4j.Document;
import org.dom4j.DocumentException;
import org.dom4j.DocumentHelper;
import org.dom4j.Element;

public class SparkCountByAgeMetagetter implements OutputMetadataGetter {

	@Override
	public List<PortMetadata> getOutputMetadatas(String operatorXml,
			List<PortMetadata> inPorts) {
		List<FieldMetadata> outport1Fields = new ArrayList<FieldMetadata>();

		if (inPorts != null && inPorts.size() > 0) {
			List<FieldMetadata> inport1Fields = inPorts.get(0)
					.getFieldMetadatas();
			for (FieldMetadata field : inport1Fields) {
				outport1Fields.add(field);
			}
		}
		
		Integer parseType = 0;
		Document document = null;
		Element opRoot = null;
		try {
			document = DocumentHelper.parseText(operatorXml);
		} catch (DocumentException e) {
			e.printStackTrace();
		}
		opRoot = document.getRootElement();
		Iterator<?> opParameters = opRoot.elementIterator("parameter");
		while (opParameters.hasNext()) {
			Element paraElt = (Element) opParameters.next();
			String parameterName = paraElt.attributeValue("name");
			if (parameterName.equals("parse_type")) {
				parseType = Integer.valueOf(paraElt.getTextTrim().trim());
				break;
			}
		}
		
		if (parseType == 7) {
			outport1Fields.add(new FieldMetadata("age", DataType.INTEGER));
			outport1Fields.add(new FieldMetadata("count", DataType.INTEGER));
		}

		PortMetadata outport1 = new PortMetadata("outport1");
		outport1.setFieldMetadatas(outport1Fields);
		
		List<PortMetadata> outportList = new ArrayList<PortMetadata>();
		outportList.add(outport1);
		
		return outportList;
	}

}
