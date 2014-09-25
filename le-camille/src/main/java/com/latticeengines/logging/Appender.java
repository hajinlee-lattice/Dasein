package com.latticeengines.logging;

/**
 * IAppenders implement logging callback methods.
 * IAppenders should not use loggers themselves.
 */
public interface Appender {
	public void info (String logger, String message);
	public void debug(String logger, String message);
	public void trace(String logger, String message);
	public void warn (String logger, String message);
	public void error(String logger, String message);
}
