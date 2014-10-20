package com.latticeengines.logging;

import java.io.IOException;
import java.util.TimeZone;

import org.apache.commons.lang3.time.FastDateFormat;

public class DefaultAppender implements Appender {
    private static final FastDateFormat formatter = FastDateFormat.getInstance("yyyy-MM-dd HH:mm:ss.SSS z",
            TimeZone.getTimeZone("GMT"));

    private Appendable out;

    public DefaultAppender(Appendable out) {
        this.out = out;
    }

    protected void append(String label, String loggerName, String message) {
        if (out != null) {
            try {
                out.append(String.format("%s | %s | %s | %s\n", formatter.format(System.currentTimeMillis()), label,
                        loggerName, message));
            } catch (IOException e0) {
                try {
                    out.append(LoggerAdapter.stackTraceToString(e0));
                } catch (IOException e1) {
                    e1.printStackTrace();
                }
            }
        }
    }

    @Override
    public void info(String c, String s) {
        append("[INFO] ", c, s);
    }

    @Override
    public void debug(String c, String s) {
        append("[DEBUG]", c, s);
    }

    @Override
    public void trace(String c, String s) {
        append("[TRACE]", c, s);
    }

    @Override
    public void warn(String c, String s) {
        append("[WARN] ", c, s);
    }

    @Override
    public void error(String c, String s) {
        append("[ERROR]", c, s);
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((out == null) ? 0 : out.hashCode());
        return result;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj)
            return true;
        if (obj == null)
            return false;
        if (!(obj instanceof DefaultAppender))
            return false;
        DefaultAppender other = (DefaultAppender) obj;
        if (out == null) {
            if (other.out != null)
                return false;
        } else if (!out.equals(other.out))
            return false;
        return true;
    }
}
