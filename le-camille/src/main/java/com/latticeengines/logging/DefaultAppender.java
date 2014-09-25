package com.latticeengines.logging;

import java.io.IOException;
import java.util.TimeZone;

import org.apache.commons.lang3.time.FastDateFormat;

public class DefaultAppender implements Appender {
	private static final FastDateFormat formatter = FastDateFormat.getInstance("yyyy-MM-dd HH:mm:ss.SSS z", TimeZone.getTimeZone("GMT"));
	
	private Appendable out;
	
	public DefaultAppender(Appendable out) {
		this.out = out;
	}
	
	protected void append(String label, String loggerName, String message) {
		if (out != null) {
			try {
				out.append(String.format("%s | %s | %s | %s\n", formatter.format(System.currentTimeMillis()), label, loggerName, message));
			}
			catch (IOException e0) {
				try {
					out.append(LoggerAdapter.stackTraceToString(e0));
				}
				catch (IOException e1) {
					e1.printStackTrace();
				}
			}
		}
	}
	
	@Override public void info (String c,String s){append("[INFO] ",c,s);}
	@Override public void debug(String c,String s){append("[DEBUG]",c,s);}
	@Override public void trace(String c,String s){append("[TRACE]",c,s);}
	@Override public void warn (String c,String s){append("[WARN] ",c,s);}
	@Override public void error(String c,String s){append("[ERROR]",c,s);}
}
