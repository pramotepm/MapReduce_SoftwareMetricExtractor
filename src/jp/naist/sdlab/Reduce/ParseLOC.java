package jp.naist.sdlab.Reduce;

public class ParseLOC {
	public static String parse(String content) {
		String[] t = content.split("\n");
		return String.valueOf(Integer.parseInt(t[t.length - 1])) + ",";
	}
}
