package jp.naist.sdlab.Reduce;

import java.io.StringReader;
import java.util.LinkedList;
import java.util.List;

import org.jdom2.Document;
import org.jdom2.Element;
import org.jdom2.input.SAXBuilder;

public class JavaNCSS {
	private static String toCSV(String[] metrics) {
		StringBuilder s = new StringBuilder("");
		for (String m : metrics) {
			s.append(m);
			s.append(",");
		}
		return s.toString();
	}
	
	public static String extract(String xmlString) {
		List<String> m = new LinkedList<String>();
		try {
			SAXBuilder builder = new SAXBuilder();
			Document document = (Document) builder.build(new StringReader(xmlString));
			Element rootNode = document.getRootElement();
			
			String wpInnerClass = rootNode.getChild("packages").getChild("total").getChild("classes").getText();
			String wpFunction = rootNode.getChild("packages").getChild("total").getChild("functions").getText();
			String wpNCSS = rootNode.getChild("packages").getChild("total").getChild("ncss").getText();
			String wpJavaDocs = rootNode.getChild("packages").getChild("total").getChild("javadocs").getText();
			m.add(wpInnerClass);
			m.add(wpFunction);
			m.add(wpNCSS);
			m.add(wpJavaDocs);

			List<Element> table = rootNode.getChild("packages").getChild("table").getChildren("tr").get(3).getChildren("td");
			String avFunction = table.get(2).getText();
			String avNCSS = table.get(3).getText();
			String avJavaDocs = table.get(4).getText();
			m.add(avFunction);
			m.add(avNCSS);
			m.add(avJavaDocs);
			
			int sumNCSS = 0;
			int	sumCCN = 0;
			int	sumJVDC = 0;
			List<Element> f = rootNode.getChild("functions").getChildren("function");
			for (int i=0; i<f.size(); i++) {
				sumNCSS += Integer.parseInt(f.get(i).getChild("ncss").getText());
				sumCCN += Integer.parseInt(f.get(i).getChild("ccn").getText());
				sumJVDC += Integer.parseInt(f.get(i).getChild("javadocs").getText());
			}
			m.add(String.valueOf(sumNCSS));
			m.add(String.valueOf(sumCCN));
			m.add(String.valueOf(sumJVDC));
		} catch (Exception e) {
			e.printStackTrace();
		}
		return toCSV(m.toArray(new String[m.size()]));
	}
}