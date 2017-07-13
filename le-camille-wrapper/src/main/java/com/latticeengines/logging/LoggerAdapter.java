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
import java.util.concurrent.ThreadFactory;

import org.slf4j.ILoggerFactory;
import org.slf4j.Logger;
import org.slf4j.Marker;

public final class LoggerAdapter {
    // for fast, thread safe iteration
    private static final Collection<Appender> appendersList = new CopyOnWriteArrayList<Appender>();

    // to ensure uniqueness
    private static final Collection<Appender> appendersSet = new HashSet<Appender>();

    // single daemon logging thread ensures log ordering
    private static final ExecutorService executor = Executors.newSingleThreadExecutor(new ThreadFactory() {
        @Override
        public Thread newThread(Runnable r) {
            Thread t = new Thread(r);
            t.setDaemon(true);
            return t;
        }
    });

    // at system shutdown, kill the logging thread and await its' termination
    static {
        Runtime.getRuntime().addShutdownHook(new Thread() {
            @Override
            public void run() {
                synchronized (appendersSet) {
                    sync();
                    executor.shutdownNow();
                }
            }
        });
    }

    // prevent instantiation
    private LoggerAdapter() {
    }

    public static String stackTraceToString(Throwable t) {
        StringWriter sw = new StringWriter();
        t.printStackTrace(new PrintWriter(sw));
        return sw.toString();
    }

    public static ILoggerFactory newLoggerFactory() {
        return new ILoggerFactory() {
            private Map<String, Logger> loggerMap = new HashMap<String, Logger>();

            @Override
            public LoggerFactory.getLogger(String name) {
                Logger logger;
                synchronized (loggerMap) {
                    if ((logger = loggerMap.get(name)) == null) {
                        // Logging is not supported in Appender(s) as that could
                        // cause infinite loops.
                        // Before creating a logger that makes callbacks to
                        // Appenders we should be sure
                        // it's attempted use is not in an Appender.
                        // If Appender is implemented, return a logger that does
                        // not make the callbacks
                        // (NoOpLogger) to prevent runtime exceptions.

                        boolean foundAppender = false;

                        try {
                            for (Class<?> c = Class.forName(name); c != null; c = c.getEnclosingClass()) {
                                if (Appender.class.isAssignableFrom(c)) {
                                    foundAppender = true;
                                    break;
                                }
                            }
                        } catch (ClassNotFoundException e0) { // non-class
                                                              // naming is OK
                                                              // (e.g., "ROOT")
                            for (StackTraceElement e : new Throwable().getStackTrace()) {
                                try {
                                    if (Appender.class.isAssignableFrom(Class.forName(e.getClassName()))) {
                                        foundAppender = true;
                                        break;
                                    }
                                } catch (ClassNotFoundException e1) {
                                } // can't happen, just satisfying compiler
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
                public void run() {
                }
            }).get();
        } catch (InterruptedException | ExecutionException e) {
            e.printStackTrace();
        }
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
        private static String format(String format, Object... args) {
            if (format == null)
                return null;
            try {
                return org.slf4j.helpers.MessageFormatter.arrayFormat(format, args).getMessage();
            } catch (Exception e) {
                return stackTraceToString(e);
            }
        }

        private void fireDebug(final String message, final Throwable t) {
            final Iterator<Appender> iter = appendersList.iterator();
            executor.submit(new Runnable() {
                @Override
                public void run() {
                    String m = messageWithStackTrace(message, t);
                    while (iter.hasNext()) {
                        try {
                            iter.next().debug(name, m);
                        } catch (Exception e) {
                        }
                    }
                }
            });
        }

        private void fireDebug(final String format, final Object... args) {
            final Iterator<Appender> iter = appendersList.iterator();
            executor.submit(new Runnable() {
                @Override
                public void run() {
                    String message = format(format, args);
                    while (iter.hasNext()) {
                        try {
                            iter.next().debug(name, message);
                        } catch (Exception e) {
                        }
                    }
                }
            });
        }

        private void fireError(final String message, final Throwable t) {
            final Iterator<Appender> iter = appendersList.iterator();
            executor.submit(new Runnable() {
                @Override
                public void run() {
                    String m = messageWithStackTrace(message, t);
                    while (iter.hasNext()) {
                        try {
                            iter.next().error(name, m);
                        } catch (Exception e) {
                        }
                    }
                }
            });
        }

        private void fireError(final String format, final Object... args) {
            final Iterator<Appender> iter = appendersList.iterator();
            executor.submit(new Runnable() {
                @Override
                public void run() {
                    String message = format(format, args);
                    while (iter.hasNext()) {
                        try {
                            iter.next().error(name, message);
                        } catch (Exception e) {
                        }
                    }
                }
            });
        }

        private void fireInfo(final String message, final Throwable t) {
            final Iterator<Appender> iter = appendersList.iterator();
            executor.submit(new Runnable() {
                @Override
                public void run() {
                    String m = messageWithStackTrace(message, t);
                    while (iter.hasNext()) {
                        try {
                            iter.next().info(name, m);
                        } catch (Exception e) {
                        }
                    }
                }
            });
        }

        private void fireInfo(final String format, final Object... args) {
            final Iterator<Appender> iter = appendersList.iterator();
            executor.submit(new Runnable() {
                @Override
                public void run() {
                    String message = format(format, args);
                    while (iter.hasNext()) {
                        try {
                            iter.next().info(name, message);
                        } catch (Exception e) {
                        }
                    }
                }
            });
        }

        private void fireTrace(final String message, final Throwable t) {
            final Iterator<Appender> iter = appendersList.iterator();
            executor.submit(new Runnable() {
                @Override
                public void run() {
                    String m = messageWithStackTrace(message, t);
                    while (iter.hasNext()) {
                        try {
                            iter.next().trace(name, m);
                        } catch (Exception e) {
                        }
                    }
                }
            });
        }

        private void fireTrace(final String format, final Object... args) {
            final Iterator<Appender> iter = appendersList.iterator();
            executor.submit(new Runnable() {
                @Override
                public void run() {
                    String message = format(format, args);
                    while (iter.hasNext()) {
                        try {
                            iter.next().trace(name, message);
                        } catch (Exception e) {
                        }
                    }
                }
            });
        }

        private void fireWarn(final String message, final Throwable t) {
            final Iterator<Appender> iter = appendersList.iterator();
            executor.submit(new Runnable() {
                @Override
                public void run() {
                    String m = messageWithStackTrace(message, t);
                    while (iter.hasNext()) {
                        try {
                            iter.next().warn(name, m);
                        } catch (Exception e) {
                        }
                    }
                }
            });
        }

        private void fireWarn(final String format, final Object... args) {
            final Iterator<Appender> iter = appendersList.iterator();
            executor.submit(new Runnable() {
                @Override
                public void run() {
                    String message = format(format, args);
                    while (iter.hasNext()) {
                        try {
                            iter.next().warn(name, message);
                        } catch (Exception e) {
                        }
                    }
                }
            });
        }

        @Override
        public boolean isDebugEnabled() {
            return true;
        }

        @Override
        public boolean isDebugEnabled(Marker arg0) {
            return true;
        }

        @Override
        public boolean isErrorEnabled() {
            return true;
        }

        @Override
        public boolean isErrorEnabled(Marker arg0) {
            return true;
        }

        @Override
        public boolean isInfoEnabled() {
            return true;
        }

        @Override
        public boolean isInfoEnabled(Marker arg0) {
            return true;
        }

        @Override
        public boolean isTraceEnabled() {
            return true;
        }

        @Override
        public boolean isTraceEnabled(Marker arg0) {
            return true;
        }

        @Override
        public boolean isWarnEnabled() {
            return true;
        }

        @Override
        public boolean isWarnEnabled(Marker arg0) {
            return true;
        }

        @Override
        public void debug(String arg0) {
            fireDebug(arg0);
        }

        @Override
        public void debug(String arg0, Object arg1) {
            fireDebug(arg0, arg1);
        }

        @Override
        public void debug(String arg0, Object... arg1) {
            fireDebug(arg0, arg1);
        }

        @Override
        public void debug(String arg0, Throwable arg1) {
            fireDebug(arg0, arg1);
        }

        @Override
        public void debug(Marker arg0, String arg1) {
            fireDebug(arg1);
        }

        @Override
        public void debug(String arg0, Object arg1, Object arg2) {
            fireDebug(arg0, arg1, arg2);
        }

        @Override
        public void debug(Marker arg0, String arg1, Object arg2) {
            fireDebug(arg1, arg2);
        }

        @Override
        public void debug(Marker arg0, String arg1, Object... arg2) {
            fireDebug(arg1, arg2);
        }

        @Override
        public void debug(Marker arg0, String arg1, Throwable arg2) {
            fireDebug(arg1, arg2);
        }

        @Override
        public void debug(Marker arg0, String arg1, Object arg2, Object arg3) {
            fireDebug(arg1, arg2, arg3);
        }

        @Override
        public void error(String arg0) {
            fireError(arg0);
        }

        @Override
        public void error(String arg0, Object arg1) {
            fireError(arg0, arg1);
        }

        @Override
        public void error(String arg0, Object... arg1) {
            fireError(arg0, arg1);
        }

        @Override
        public void error(String arg0, Throwable arg1) {
            fireError(arg0, arg1);
        }

        @Override
        public void error(Marker arg0, String arg1) {
            fireError(arg1);
        }

        @Override
        public void error(String arg0, Object arg1, Object arg2) {
            fireError(arg0, arg1, arg2);
        }

        @Override
        public void error(Marker arg0, String arg1, Object arg2) {
            fireError(arg1, arg2);
        }

        @Override
        public void error(Marker arg0, String arg1, Object... arg2) {
            fireError(arg1, arg2);
        }

        @Override
        public void error(Marker arg0, String arg1, Throwable arg2) {
            fireError(arg1, arg2);
        }

        @Override
        public void error(Marker arg0, String arg1, Object arg2, Object arg3) {
            fireError(arg1, arg2, arg3);
        }

        @Override
        public void info(String arg0) {
            fireInfo(arg0);
        }

        @Override
        public void info(String arg0, Object arg1) {
            fireInfo(arg0, arg1);
        }

        @Override
        public void info(String arg0, Object... arg1) {
            fireInfo(arg0, arg1);
        }

        @Override
        public void info(String arg0, Throwable arg1) {
            fireInfo(arg0, arg1);
        }

        @Override
        public void info(Marker arg0, String arg1) {
            fireInfo(arg1);
        }

        @Override
        public void info(String arg0, Object arg1, Object arg2) {
            fireInfo(arg0, arg1, arg2);
        }

        @Override
        public void info(Marker arg0, String arg1, Object arg2) {
            fireInfo(arg1, arg2);
        }

        @Override
        public void info(Marker arg0, String arg1, Object... arg2) {
            fireInfo(arg1, arg2);
        }

        @Override
        public void info(Marker arg0, String arg1, Throwable arg2) {
            fireInfo(arg1, arg2);
        }

        @Override
        public void info(Marker arg0, String arg1, Object arg2, Object arg3) {
            fireInfo(arg1, arg2, arg3);
        }

        @Override
        public void trace(String arg0) {
            fireTrace(arg0);
        }

        @Override
        public void trace(String arg0, Object arg1) {
            fireTrace(arg0, arg1);
        }

        @Override
        public void trace(String arg0, Object... arg1) {
            fireTrace(arg0, arg1);
        }

        @Override
        public void trace(String arg0, Throwable arg1) {
            fireTrace(arg0, arg1);
        }

        @Override
        public void trace(Marker arg0, String arg1) {
            fireTrace(arg1);
        }

        @Override
        public void trace(String arg0, Object arg1, Object arg2) {
            fireTrace(arg0, arg1, arg2);
        }

        @Override
        public void trace(Marker arg0, String arg1, Object arg2) {
            fireTrace(arg1, arg2);
        }

        @Override
        public void trace(Marker arg0, String arg1, Object... arg2) {
            fireTrace(arg1, arg2);
        }

        @Override
        public void trace(Marker arg0, String arg1, Throwable arg2) {
            fireTrace(arg1, arg2);
        }

        @Override
        public void trace(Marker arg0, String arg1, Object arg2, Object arg3) {
            fireTrace(arg1, arg2, arg3);
        }

        @Override
        public void warn(String arg0) {
            fireWarn(arg0);
        }

        @Override
        public void warn(String arg0, Object arg1) {
            fireWarn(arg0, arg1);
        }

        @Override
        public void warn(String arg0, Object... arg1) {
            fireWarn(arg0, arg1);
        }

        @Override
        public void warn(String arg0, Throwable arg1) {
            fireWarn(arg0, arg1);
        }

        @Override
        public void warn(Marker arg0, String arg1) {
            fireWarn(arg1);
        }

        @Override
        public void warn(String arg0, Object arg1, Object arg2) {
            fireWarn(arg0, arg1, arg2);
        }

        @Override
        public void warn(Marker arg0, String arg1, Object arg2) {
            fireWarn(arg1, arg2);
        }

        @Override
        public void warn(Marker arg0, String arg1, Object... arg2) {
            fireWarn(arg1, arg2);
        }

        @Override
        public void warn(Marker arg0, String arg1, Throwable arg2) {
            fireWarn(arg1, arg2);
        }

        @Override
        public void warn(Marker arg0, String arg1, Object arg2, Object arg3) {
            fireWarn(arg1, arg2, arg3);
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

        @Override
        public boolean isTraceEnabled() {
            return false;
        }

        @Override
        public void trace(String msg) {
        }

        @Override
        public void trace(String format, Object arg) {
        }

        @Override
        public void trace(String format, Object arg1, Object arg2) {
        }

        @Override
        public void trace(String format, Object... arguments) {
        }

        @Override
        public void trace(String msg, Throwable t) {
        }

        @Override
        public boolean isTraceEnabled(Marker marker) {
            return false;
        }

        @Override
        public void trace(Marker marker, String msg) {
        }

        @Override
        public void trace(Marker marker, String format, Object arg) {
        }

        @Override
        public void trace(Marker marker, String format, Object arg1, Object arg2) {
        }

        @Override
        public void trace(Marker marker, String format, Object... argArray) {
        }

        @Override
        public void trace(Marker marker, String msg, Throwable t) {
        }

        @Override
        public boolean isDebugEnabled() {
            return false;
        }

        @Override
        public void debug(String msg) {
        }

        @Override
        public void debug(String format, Object arg) {
        }

        @Override
        public void debug(String format, Object arg1, Object arg2) {
        }

        @Override
        public void debug(String format, Object... arguments) {
        }

        @Override
        public void debug(String msg, Throwable t) {
        }

        @Override
        public boolean isDebugEnabled(Marker marker) {
            return false;
        }

        @Override
        public void debug(Marker marker, String msg) {
        }

        @Override
        public void debug(Marker marker, String format, Object arg) {
        }

        @Override
        public void debug(Marker marker, String format, Object arg1, Object arg2) {
        }

        @Override
        public void debug(Marker marker, String format, Object... arguments) {
        }

        @Override
        public void debug(Marker marker, String msg, Throwable t) {
        }

        @Override
        public boolean isInfoEnabled() {
            return false;
        }

        @Override
        public void info(String msg) {
        }

        @Override
        public void info(String format, Object arg) {
        }

        @Override
        public void info(String format, Object arg1, Object arg2) {
        }

        @Override
        public void info(String format, Object... arguments) {
        }

        @Override
        public void info(String msg, Throwable t) {
        }

        @Override
        public boolean isInfoEnabled(Marker marker) {
            return false;
        }

        @Override
        public void info(Marker marker, String msg) {
        }

        @Override
        public void info(Marker marker, String format, Object arg) {
        }

        @Override
        public void info(Marker marker, String format, Object arg1, Object arg2) {
        }

        @Override
        public void info(Marker marker, String format, Object... arguments) {
        }

        @Override
        public void info(Marker marker, String msg, Throwable t) {
        }

        @Override
        public boolean isWarnEnabled() {
            return false;
        }

        @Override
        public void warn(String msg) {
        }

        @Override
        public void warn(String format, Object arg) {
        }

        @Override
        public void warn(String format, Object... arguments) {
        }

        @Override
        public void warn(String format, Object arg1, Object arg2) {
        }

        @Override
        public void warn(String msg, Throwable t) {
        }

        @Override
        public boolean isWarnEnabled(Marker marker) {
            return false;
        }

        @Override
        public void warn(Marker marker, String msg) {
        }

        @Override
        public void warn(Marker marker, String format, Object arg) {
        }

        @Override
        public void warn(Marker marker, String format, Object arg1, Object arg2) {
        }

        @Override
        public void warn(Marker marker, String format, Object... arguments) {
        }

        @Override
        public void warn(Marker marker, String msg, Throwable t) {
        }

        @Override
        public boolean isErrorEnabled() {
            return false;
        }

        @Override
        public void error(String msg) {
        }

        @Override
        public void error(String format, Object arg) {
        }

        @Override
        public void error(String format, Object arg1, Object arg2) {
        }

        @Override
        public void error(String format, Object... arguments) {
        }

        @Override
        public void error(String msg, Throwable t) {
        }

        @Override
        public boolean isErrorEnabled(Marker marker) {
            return false;
        }

        @Override
        public void error(Marker marker, String msg) {
        }

        @Override
        public void error(Marker marker, String format, Object arg) {
        }

        @Override
        public void error(Marker marker, String format, Object arg1, Object arg2) {
        }

        @Override
        public void error(Marker marker, String format, Object... arguments) {
        }

        @Override
        public void error(Marker marker, String msg, Throwable t) {
        }
    }
}
