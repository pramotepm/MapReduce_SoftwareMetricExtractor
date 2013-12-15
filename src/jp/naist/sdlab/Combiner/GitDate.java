package jp.naist.sdlab.Combiner;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.TimeZone;

public class GitDate {
	
	Date gd;
	SimpleDateFormat sdf;
	String shortDate;
	
	public GitDate(String DateString) {
		sdf = new SimpleDateFormat("EEE MMM dd HH:mm:ss yyyy Z");
		sdf.setTimeZone(TimeZone.getTimeZone("UTC"));
		try {
			gd = sdf.parse(DateString);
		} catch (ParseException e) {
			e.printStackTrace();
		}
		Calendar calendar;
		calendar = Calendar.getInstance();
		calendar.setTime(gd);
		shortDate = String.format("%1$tY-%1$tm-%1$td", calendar); 
	}
	
	public String getShortDate() {
		return shortDate; 
	}

	public Date getDate() {
		return gd;
	}
	
	public boolean isDateFresherThan(GitDate gd2) {
		return gd.compareTo(gd2.getDate()) > 0;
	}
}