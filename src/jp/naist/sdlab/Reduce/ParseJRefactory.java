package jp.naist.sdlab.Reduce;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map.Entry;
import java.util.StringTokenizer;

public class ParseJRefactory {

	private static class ClassStruct {
		public String className;
		public LinkedList<MethodStruct> methods;

		public ClassStruct(String name) {
			this.className = name;
			this.methods = new LinkedList<MethodStruct>();
		}

		public String getName() {
			return className;
		}
	}

	private static class MethodStruct {
		public String methodName;
		public LinkedList<String> attrs;

		public MethodStruct(String name) {
			this.methodName = name;
			this.attrs = new LinkedList<String>();
		}

		public String getName() {
			return methodName;
		}
	}

	private static HashMap<String, ClassStruct> eachClassMetrics;
	private static HashMap<String, LinkedList<String>> classMetrics;
	private static LinkedList<String> restMetrics;

	private static void parseRecord(String[] records) {
		ClassStruct cs = null;
		MethodStruct ms = null;
		for (int i=0;i<records.length;i++) {
			String[] temp = records[i].split(",");
			//String keyMetric = temp[0];
			String valueMetric = temp[1];
			String className = temp[3];
			String methodName = temp[4];
			if (i == 0) {
				if (!eachClassMetrics.containsKey(className)) 
					eachClassMetrics.put(className, new ClassStruct(className));
				cs = eachClassMetrics.get(className);
				cs.methods.add((ms = new MethodStruct(methodName)));
			}
			//ms.attrs.add(keyMetric + ":" + valueMetric);
			ms.attrs.add(valueMetric);
		}
	}

	private static String getValue(String rec) {
		return rec.split(",")[1];
	}

	private static String getClassName(String rec) {
		return rec.split(",")[3];
	}

	private static boolean process(String input) {
		try {
			StringTokenizer token = new StringTokenizer(input);
			token.nextToken();
			token.nextToken();
			token.nextToken();
			while (token.hasMoreTokens()) {
				String firstToken = token.nextToken();
				if (firstToken.startsWith("001")) {
					String nPublicMethods = getValue(firstToken);
					String nNonPublicMethods = getValue(token.nextToken());
					String nStaticMethods = getValue(token.nextToken());
					String nInstanceVariables = getValue(token.nextToken());
					String nClassVariables = getValue(token.nextToken());

					LinkedList<String> classMetricValue = new LinkedList<String>();
					classMetricValue.add(nPublicMethods);
					classMetricValue.add(nNonPublicMethods);
					classMetricValue.add(nStaticMethods);
					classMetricValue.add(nInstanceVariables);
					classMetricValue.add(nClassVariables);

					classMetrics.put(getClassName(firstToken), classMetricValue);
				}
				else if (firstToken.startsWith("000") && firstToken.endsWith("total")) {
					// ----------------------------------------------------------- //
					String totalStatements = getValue(firstToken);
					String avgTotalStatements = getValue(token.nextToken());
					restMetrics.add(totalStatements);
					restMetrics.add(avgTotalStatements);
					// ------------------------------------------------------------//
					String totalPublicMethods = getValue(token.nextToken());
					String avgPublicMethods = getValue(token.nextToken());
					restMetrics.add(totalPublicMethods);
					restMetrics.add(avgPublicMethods);
					// ------------------------------------------------------------//
					String totalOtherMethods = getValue(token.nextToken());
					String avgOtherMethos = getValue(token.nextToken());
					restMetrics.add(totalOtherMethods);
					restMetrics.add(avgOtherMethos);
					// ------------------------------------------------------------//
					String totalClassMethods = getValue(token.nextToken());
					String avgClassMethods = getValue(token.nextToken());
					restMetrics.add(totalClassMethods);
					restMetrics.add(avgClassMethods);
					// ------------------------------------------------------------//
					String totalInstanceVariables = getValue(token.nextToken());
					String avgInstanceVariables = getValue(token.nextToken());
					restMetrics.add(totalInstanceVariables);
					restMetrics.add(avgInstanceVariables);
					// ------------------------------------------------------------//
					String totalClassVariables = getValue(token.nextToken());
					String avgClassVariables = getValue(token.nextToken());
					restMetrics.add(totalClassVariables);
					restMetrics.add(avgClassVariables);
					// ------------------------------------------------------------//
					String totalAbstractClasses = getValue(token.nextToken());
					String avgAbstractClasses = getValue(token.nextToken());
					restMetrics.add(totalAbstractClasses);
					restMetrics.add(avgAbstractClasses);
					// ------------------------------------------------------------//
					String totalInterfaces = getValue(token.nextToken());
					String avgInterfaces = getValue(token.nextToken());
					restMetrics.add(totalInterfaces);
					restMetrics.add(avgInterfaces);
					// ------------------------------------------------------------//
					String totalNumberOfParameters = getValue(token.nextToken());
					String avgNumberOfParameters = getValue(token.nextToken());
					restMetrics.add(totalNumberOfParameters);
					restMetrics.add(avgNumberOfParameters);
					// ----------------------------------------------------------- //				
				}
				else if (firstToken.startsWith("000")) {
					String second = token.nextToken();
					String third = token.nextToken();
					String fourth = token.nextToken();
					parseRecord(new String[] { firstToken, second, third, fourth });
				}
			}
		}
		catch (Exception e) {
			e.printStackTrace();
			return false;
		}
		return true;
	}

	public static String parse(String metricFromJRefactory) {
		eachClassMetrics = new HashMap<String, ClassStruct>();
		classMetrics = new HashMap<String, LinkedList<String>>();
		restMetrics = new LinkedList<String>();

		if (process(metricFromJRefactory) == false) {
			System.out.println(metricFromJRefactory);
			return "";
		}
		boolean isEmptyClass = true;
		StringBuilder sb = new StringBuilder();
		for (Entry<String, ClassStruct>  eachClassMetric : eachClassMetrics.entrySet()) {
			isEmptyClass = false;
			ClassStruct classStruct = eachClassMetric.getValue();
			String className = classStruct.getName(); 
			sb.append("{");
			sb.append(className);
			for (MethodStruct methods : classStruct.methods) {
				if (methods.attrs.size() > 0) {
					sb.append(";{");
					sb.append(methods.getName());
					sb.append(",[");
					sb.append(methods.attrs.get(0));
					for (int i=1; i<methods.attrs.size(); i++) {
						sb.append(",");
						sb.append(methods.attrs.get(i));
					}
					sb.append("]}");
				}
			}
			for (String value : classMetrics.get(className)) {
				sb.append(";");
				sb.append(value);
			}
			sb.append("},");
		}
		if (isEmptyClass) {
			for (Entry<String, LinkedList<String>> c : classMetrics.entrySet()) {
				sb.append("{");
				sb.append(c.getKey());
				if (c.getValue().size() > 0) {
					sb.append(",");
					sb.append(c.getValue().get(0));
					for (int i=1; i<c.getValue().size(); i++) {
						sb.append(",");
						sb.append(c.getValue().get(i));
					}
				}
				sb.append("},");
			}
		}
		for (String allClassValue : restMetrics) {
			sb.append(allClassValue);
			sb.append(",");
		}
		return sb.toString();
	}
}