package com.latticeengines.logging;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.slf4j.ILoggerFactory;
import org.slf4j.Logger;
import org.slf4j.Marker;

public final class LoggerAdapter {
	// for fast, thread safe iteration
	private static final Collection<Appender> appendersList = new CopyOnWriteArrayList<Appender>();
	
	// to ensure uniqueness
	private static final Collection<Appender> appendersSet = new HashSet<Appender>();
	
	// single logging thread ensures log ordering
	private static final ExecutorService executor = Executors.newSingleThreadExecutor();
	
	// at system shutdown, kill the logging thread and await its' termination
	static {
		Runtime.getRuntime().addShutdownHook(new Thread() {
			@Override
			public void run() {
				sync();
				executor.shutdownNow();
			}
		});
	}
	
	// prevent instantiation
	private LoggerAdapter() {}
	
	public static String stackTraceToString(Throwable t) {
		StringWriter sw = new StringWriter();
		t.printStackTrace(new PrintWriter(sw));
		return sw.toString();
	}
	
    public static ILoggerFactory newLoggerFactory() {
    	return new ILoggerFactory() {
            private Map<String, Logger> loggerMap  = new HashMap<String, Logger>();
            
            @Override
            public Logger getLogger(String name) {
            	Logger logger;
            	synchronized (loggerMap) {
	                if ((logger = loggerMap.get(name)) == null) {
    					// Logging is not supported in Appender(s) as that could cause infinite loops.
	                	// Before creating a logger that makes callbacks to Appenders we should be sure
	                	// it's attempted use is not in an Appender.
	                	// If Appender is implemented, return a logger that does not make the callbacks
	                	// (NoOpLogger) to prevent runtime exceptions.
	                	
	                	boolean foundAppender = false;
	                	
	                	try {
							for (Class<?> c = Class.forName(name); c != null; c = c.getEnclosingClass()) {
			    				if (Appender.class.isAssignableFrom(c)) {
			    					foundAppender = true;
			    					break;
			    				}
							}
						}
	                	catch (ClassNotFoundException e0) { // non-class naming is OK (e.g., "ROOT")
	            			for (StackTraceElement e : new Throwable().getStackTrace()) {
								try {
									if (Appender.class.isAssignableFrom(Class.forName(e.getClassName()))) {
										foundAppender = true;
										break;
									}
								}
	            				catch (ClassNotFoundException e1) { } // can't happen, just satisfying compiler
	            			}
	                	}
	                	
	                	loggerMap.put(name, logger = foundAppender ? new NoOpLogger(name) : new LoggerImpl(name));
	                }
            	}
	            return logger;
            }
        };
    }
	
    /**
     * Blocks until the logging thread is done.
     */
    public static void sync() {
    	try {
    		// submit an empty job and wait for it to finish
        	executor.submit(new Runnable() {
    			@Override
    			public void run() { }
        	}).get();
		}
    	catch (InterruptedException e) {
			e.printStackTrace();
		}
    	catch (ExecutionException e) {
			e.printStackTrace();
		}
    }
    
	private static void fireDebug(final String name, final String message) {
		final Iterator<Appender> iter = appendersList.iterator();
		executor.submit(new Runnable() {
			@Override
			public void run() {
				while (iter.hasNext()) {
					Appender a = iter.next();
					try {
						a.debug(name, message);
					}
					catch (Exception e) {}
				}
			}
		});
	}
	
	private static void fireError(final String name, final String message) {
		final Iterator<Appender> iter = appendersList.iterator();
		executor.submit(new Runnable() {
			@Override
			public void run() {
				while (iter.hasNext()) {
					Appender a = iter.next();
					try {
						a.error(name, message);
					}
					catch (Exception e) {}
				}
			}
		});
	}
	
	private static void fireInfo(final String name, final String message) {
		final Iterator<Appender> iter = appendersList.iterator();
		executor.submit(new Runnable() {
			@Override
			public void run() {
				while (iter.hasNext()) {
					Appender a = iter.next();
					try {
						a.info(name, message);
					}
					catch (Exception e) {}
				}
			}
		});
	}
	
	private static void fireTrace(final String name, final String message) {
		final Iterator<Appender> iter = appendersList.iterator();
		executor.submit(new Runnable() {
			@Override
			public void run() {
				while (iter.hasNext()) {
					Appender a = iter.next();
					try {
						a.trace(name, message);
					}
					catch (Exception e) {}
				}
			}
		});
	}
	
	private static void fireWarn(final String name, final String message) {
		final Iterator<Appender> iter = appendersList.iterator();
		executor.submit(new Runnable() {
			@Override
			public void run() {
				while (iter.hasNext()) {
					Appender a = iter.next();
					try {
						a.warn(name, message);
					}
					catch (Exception e) {}
				}
			}
		});
	}
	
	public static boolean addAppender(Appender appender) {
		if (appender == null)
			return false;
			
		synchronized (appendersSet) {
			if (appendersSet.contains(appender)) {
				return false;
			}
			appendersList.add(appender);
			appendersSet.add(appender);
		}
		
		return true;
	}
	
	public static boolean removeAppender(Appender appender) {
		if (appender == null)
			return false;
		
		synchronized (appendersSet) {
			if (appendersSet.contains(appender)) {
				appendersList.remove(appender);
				appendersSet.remove(appender);
				return true;
			}
		}
		
		return false;
	}
	
	private static class LoggerImpl implements Logger {
		private String name;
		
		private boolean
			debugEnabled = true,
			errorEnabled = true,
			infoEnabled = true,
			traceEnabled = true,
			warnEnabled = true;
		
		private static String messageWithStackTrace(String message, Throwable t) {
			return message + "\n" + stackTraceToString(t);
		}
		
		LoggerImpl(String name) {
			this.name = name;
		}
		
		@Override
		public String getName() {
			return name;
		}
		
		// called by all the logging functions for string formatting
		private static String format(String format, Object ... args) {
			if (format == null)
				return null;
			try {
				return org.slf4j.helpers.MessageFormatter.arrayFormat(format, args).getMessage();
			}
			catch (Exception e) {
				return stackTraceToString(e);
			}
		}
		
		private void doDebug(String format, Object ... args) {
			if (debugEnabled) fireDebug(name, format(format, args));
		}
		
		private void doError(String format, Object ... args) {
			if (errorEnabled) fireError(name, format(format, args));
		}
		
		private void doInfo(String format, Object ... args) {
			if (infoEnabled) fireInfo(name, format(format, args));
		}
		
		private void doTrace(String format, Object ... args) {
			if (traceEnabled) fireTrace(name, format(format, args));
		}
		
		private void doWarn(String format, Object ... args) {
			if (warnEnabled) fireWarn(name, format(format, args));
		}
		
		@Override
		public boolean isDebugEnabled() {
			return debugEnabled;
		}
		
		@Override
		public boolean isDebugEnabled(Marker arg0) {
			return debugEnabled;
		}

		@Override
		public boolean isErrorEnabled() {
			return errorEnabled;
		}

		@Override
		public boolean isErrorEnabled(Marker arg0) {
			return errorEnabled;
		}

		@Override
		public boolean isInfoEnabled() {
			return infoEnabled;
		}

		@Override
		public boolean isInfoEnabled(Marker arg0) {
			return infoEnabled;
		}

		@Override
		public boolean isTraceEnabled() {
			return traceEnabled;
		}

		@Override
		public boolean isTraceEnabled(Marker arg0) {
			return traceEnabled;
		}

		@Override
		public boolean isWarnEnabled() {
			return warnEnabled;
		}

		@Override
		public boolean isWarnEnabled(Marker arg0) {
			return warnEnabled;
		}
		
		@Override
		public void debug(String arg0) {
			doDebug(arg0);
		}

		@Override
		public void debug(String arg0, Object arg1) {
			doDebug(arg0, arg1);
		}

		@Override
		public void debug(String arg0, Object... arg1) {
			doDebug(arg0, arg1);
		}

		@Override
		public void debug(String arg0, Throwable arg1) {
			doDebug(messageWithStackTrace(arg0, arg1));
		}

		@Override
		public void debug(Marker arg0, String arg1) {
			doDebug(arg1);
		}

		@Override
		public void debug(String arg0, Object arg1, Object arg2) {
			doDebug(arg0, arg1, arg2);
		}

		@Override
		public void debug(Marker arg0, String arg1, Object arg2) {
			doDebug(arg1, arg2);
		}

		@Override
		public void debug(Marker arg0, String arg1, Object... arg2) {
			doDebug(arg1, arg2);
		}

		@Override
		public void debug(Marker arg0, String arg1, Throwable arg2) {
			doDebug(messageWithStackTrace(arg1, arg2));
		}

		@Override
		public void debug(Marker arg0, String arg1, Object arg2, Object arg3) {
			doDebug(arg1, arg2, arg3);
		}

		@Override
		public void error(String arg0) {
			doError(arg0);
		}

		@Override
		public void error(String arg0, Object arg1) {
			doError(arg0, arg1);
		}

		@Override
		public void error(String arg0, Object... arg1) {
			doError(arg0, arg1);
		}

		@Override
		public void error(String arg0, Throwable arg1) {
			doError(messageWithStackTrace(arg0, arg1));
		}

		@Override
		public void error(Marker arg0, String arg1) {
			doError(arg1);
		}

		@Override
		public void error(String arg0, Object arg1, Object arg2) {
			doError(arg0, arg1, arg2);
		}

		@Override
		public void error(Marker arg0, String arg1, Object arg2) {
			doError(arg1, arg2);
		}

		@Override
		public void error(Marker arg0, String arg1, Object... arg2) {
			doError(arg1, arg2);
		}

		@Override
		public void error(Marker arg0, String arg1, Throwable arg2) {
			doError(messageWithStackTrace(arg1, arg2));
		}

		@Override
		public void error(Marker arg0, String arg1, Object arg2, Object arg3) {
			doError(arg1, arg2, arg3);
		}

		@Override
		public void info(String arg0) {
			doInfo(arg0);
		}

		@Override
		public void info(String arg0, Object arg1) {
			doInfo(arg0, arg1);
		}

		@Override
		public void info(String arg0, Object... arg1) {
			doInfo(arg0, arg1);
		}

		@Override
		public void info(String arg0, Throwable arg1) {
			doInfo(messageWithStackTrace(arg0, arg1));
		}

		@Override
		public void info(Marker arg0, String arg1) {
			doInfo(arg1);
		}

		@Override
		public void info(String arg0, Object arg1, Object arg2) {
			doInfo(arg0, arg1, arg2);
		}

		@Override
		public void info(Marker arg0, String arg1, Object arg2) {
			doInfo(arg1, arg2);
		}

		@Override
		public void info(Marker arg0, String arg1, Object... arg2) {
			doInfo(arg1, arg2);
		}

		@Override
		public void info(Marker arg0, String arg1, Throwable arg2) {
			doInfo(messageWithStackTrace(arg1, arg2));
		}

		@Override
		public void info(Marker arg0, String arg1, Object arg2, Object arg3) {
			doInfo(arg1, arg2, arg3);
		}

		@Override
		public void trace(String arg0) {
			doTrace(arg0);
		}

		@Override
		public void trace(String arg0, Object arg1) {
			doTrace(arg0, arg1);
		}

		@Override
		public void trace(String arg0, Object... arg1) {
			doTrace(arg0, arg1);
		}

		@Override
		public void trace(String arg0, Throwable arg1) {
			doTrace(messageWithStackTrace(arg0, arg1));
		}

		@Override
		public void trace(Marker arg0, String arg1) {
			doTrace(arg1);
		}

		@Override
		public void trace(String arg0, Object arg1, Object arg2) {
			doTrace(arg0, arg1, arg2);
		}

		@Override
		public void trace(Marker arg0, String arg1, Object arg2) {
			doTrace(arg1, arg2);
		}

		@Override
		public void trace(Marker arg0, String arg1, Object... arg2) {
			doTrace(arg1, arg2);
		}

		@Override
		public void trace(Marker arg0, String arg1, Throwable arg2) {
			doTrace(messageWithStackTrace(arg1, arg2));
		}

		@Override
		public void trace(Marker arg0, String arg1, Object arg2, Object arg3) {
			doTrace(arg1, arg2, arg3);
		}

		@Override
		public void warn(String arg0) {
			doWarn(arg0);
		}

		@Override
		public void warn(String arg0, Object arg1) {
			doWarn(arg0, arg1);
		}

		@Override
		public void warn(String arg0, Object... arg1) {
			doWarn(arg0, arg1);
		}

		@Override
		public void warn(String arg0, Throwable arg1) {
			doWarn(messageWithStackTrace(arg0, arg1));
		}

		@Override
		public void warn(Marker arg0, String arg1) {
			doWarn(arg1);
		}

		@Override
		public void warn(String arg0, Object arg1, Object arg2) {
			doWarn(arg0, arg1, arg2);
		}

		@Override
		public void warn(Marker arg0, String arg1, Object arg2) {
			doWarn(arg1, arg2);
		}

		@Override
		public void warn(Marker arg0, String arg1, Object... arg2) {
			doWarn(arg1, arg2);
		}

		@Override
		public void warn(Marker arg0, String arg1, Throwable arg2) {
			doWarn(messageWithStackTrace(arg1, arg2));
		}

		@Override
		public void warn(Marker arg0, String arg1, Object arg2, Object arg3) {
			doWarn(arg1, arg2, arg3);
		}
	}
	
	private static class NoOpLogger implements Logger {
		private String name;
		
		NoOpLogger(String name) {
			this.name = name;
		}
		
		@Override
		public String getName() {
			return name;
		}
		
		@Override public boolean isTraceEnabled() {return false;}
		@Override public void trace(String msg) { }
		@Override public void trace(String format, Object arg) { }
		@Override public void trace(String format, Object arg1, Object arg2) { }
		@Override public void trace(String format, Object... arguments) { }
		@Override public void trace(String msg, Throwable t) { }
		@Override public boolean isTraceEnabled(Marker marker) {return false;}
		@Override public void trace(Marker marker, String msg) { }
		@Override public void trace(Marker marker, String format, Object arg) { }
		@Override public void trace(Marker marker, String format, Object arg1, Object arg2) { }
		@Override public void trace(Marker marker, String format, Object... argArray) { }
		@Override public void trace(Marker marker, String msg, Throwable t) { }
		@Override public boolean isDebugEnabled() {return false;}
		@Override public void debug(String msg) { }
		@Override public void debug(String format, Object arg) { }
		@Override public void debug(String format, Object arg1, Object arg2) { }
		@Override public void debug(String format, Object... arguments) { }
		@Override public void debug(String msg, Throwable t) { }
		@Override public boolean isDebugEnabled(Marker marker) {return false;}
		@Override public void debug(Marker marker, String msg) { }
		@Override public void debug(Marker marker, String format, Object arg) {}
		@Override public void debug(Marker marker, String format, Object arg1, Object arg2) {}
		@Override public void debug(Marker marker, String format, Object... arguments) {}
		@Override public void debug(Marker marker, String msg, Throwable t) { }
		@Override public boolean isInfoEnabled() {return false;}
		@Override public void info(String msg) { }
		@Override public void info(String format, Object arg) { }
		@Override public void info(String format, Object arg1, Object arg2) { }
		@Override public void info(String format, Object... arguments) { }
		@Override public void info(String msg, Throwable t) { }
		@Override public boolean isInfoEnabled(Marker marker) {return false;}
		@Override public void info(Marker marker, String msg) { }
		@Override public void info(Marker marker, String format, Object arg) { }
		@Override public void info(Marker marker, String format, Object arg1, Object arg2) { }
		@Override public void info(Marker marker, String format, Object... arguments) { }
		@Override public void info(Marker marker, String msg, Throwable t) { }
		@Override public boolean isWarnEnabled() {return false;}
		@Override public void warn(String msg) { }
		@Override public void warn(String format, Object arg) { }
		@Override public void warn(String format, Object... arguments) { }
		@Override public void warn(String format, Object arg1, Object arg2) { }
		@Override public void warn(String msg, Throwable t) { }
		@Override public boolean isWarnEnabled(Marker marker) {return false;}
		@Override public void warn(Marker marker, String msg) { }
		@Override public void warn(Marker marker, String format, Object arg) { }
		@Override public void warn(Marker marker, String format, Object arg1, Object arg2) { }
		@Override public void warn(Marker marker, String format, Object... arguments) { }
		@Override public void warn(Marker marker, String msg, Throwable t) { }
		@Override public boolean isErrorEnabled() {return false;}
		@Override public void error(String msg) { }
		@Override public void error(String format, Object arg) { }
		@Override public void error(String format, Object arg1, Object arg2) { }
		@Override public void error(String format, Object... arguments) { }
		@Override public void error(String msg, Throwable t) { }
		@Override public boolean isErrorEnabled(Marker marker) {return false;}
		@Override public void error(Marker marker, String msg) { }
		@Override public void error(Marker marker, String format, Object arg) { }
		@Override public void error(Marker marker, String format, Object arg1, Object arg2) { }
		@Override public void error(Marker marker, String format, Object... arguments) { }
		@Override public void error(Marker marker, String msg, Throwable t) { }
	}
}