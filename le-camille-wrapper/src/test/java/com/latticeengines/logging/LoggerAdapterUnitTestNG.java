package com.latticeengines.logging;

import static org.testng.Assert.assertTrue;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.Marker;
import org.slf4j.MarkerFactory;
import org.testng.annotations.Test;

public class LoggerAdapterUnitTestNG {
    private static final Logger log = LoggerFactory.getLogger(new Object() {
    }.getClass().getEnclosingClass());

    private static final String message = "LoggerAdapterUnitTestNG";
    private static final String format = message + " {} {} {}";
    private static final String arg1 = "arg1";
    private static final String arg2 = "arg2";
    private static final String arg3 = "arg3";
    private static final String[] argArray = new String[] { arg1, arg2, arg3 };
    private static final Throwable throwable = new Exception("TestException");
    private static final Marker marker = MarkerFactory.getMarker("TestMarker");

    private static final int timeOutMs = 2000;

    @Test(groups = "unit")
    public void testTraceIsEnabled() {
        assertTrue(log.isTraceEnabled());
    }

    @Test(groups = "unit", timeOut = timeOutMs)
    public void testTraceMessage() {
        StringBuilder sb = new StringBuilder();
        Appender a = new DefaultAppender(sb);
        LoggerAdapter.addAppender(a);

        log.trace(message);

        LoggerAdapter.removeAppender(a);
        LoggerAdapter.sync();
        String out = sb.toString();

        assertTrue(out.contains(message));
    }

    @Test(groups = "unit", timeOut = timeOutMs)
    public void testTraceFormat1Arg() {
        StringBuilder sb = new StringBuilder();
        Appender a = new DefaultAppender(sb);
        LoggerAdapter.addAppender(a);

        log.trace(format, arg1);

        LoggerAdapter.removeAppender(a);
        LoggerAdapter.sync();
        String out = sb.toString();

        assertTrue(out.contains(message));
        assertTrue(out.contains(arg1));
    }

    @Test(groups = "unit", timeOut = timeOutMs)
    public void testTraceFormat2Args() {
        StringBuilder sb = new StringBuilder();
        Appender a = new DefaultAppender(sb);
        LoggerAdapter.addAppender(a);

        log.trace(format, arg1, arg2);

        LoggerAdapter.removeAppender(a);
        LoggerAdapter.sync();
        String out = sb.toString();

        assertTrue(out.contains(message));
        assertTrue(out.contains(arg1));
        assertTrue(out.contains(arg2));
    }

    @Test(groups = "unit", timeOut = timeOutMs)
    public void testTraceFormatArgArray() {
        StringBuilder sb = new StringBuilder();
        Appender a = new DefaultAppender(sb);
        LoggerAdapter.addAppender(a);

        log.trace(format, argArray);

        LoggerAdapter.removeAppender(a);
        LoggerAdapter.sync();
        String out = sb.toString();

        assertTrue(out.contains(message));
        assertTrue(out.contains(arg1));
        assertTrue(out.contains(arg2));
        assertTrue(out.contains(arg3));
    }

    @Test(groups = "unit", timeOut = timeOutMs)
    public void testTraceMessageThrowable() {
        StringBuilder sb = new StringBuilder();
        Appender a = new DefaultAppender(sb);
        LoggerAdapter.addAppender(a);

        log.trace(message, throwable);

        LoggerAdapter.removeAppender(a);
        LoggerAdapter.sync();
        String out = sb.toString();

        assertTrue(out.contains(message));
        assertTrue(out.contains(LoggerAdapter.stackTraceToString(throwable)));
    }

    @Test(groups = "unit")
    public void testTraceIsEnabledWithMarker() {
        assertTrue(log.isTraceEnabled(marker));
    }

    @Test(groups = "unit", timeOut = timeOutMs)
    public void testTraceMessageWithMarker() {
        StringBuilder sb = new StringBuilder();
        Appender a = new DefaultAppender(sb);
        LoggerAdapter.addAppender(a);

        log.trace(marker, message);

        LoggerAdapter.removeAppender(a);
        LoggerAdapter.sync();
        String out = sb.toString();

        assertTrue(out.contains(message));
    }

    @Test(groups = "unit", timeOut = timeOutMs)
    public void testTraceFormat1ArgWithMarker() {
        StringBuilder sb = new StringBuilder();
        Appender a = new DefaultAppender(sb);
        LoggerAdapter.addAppender(a);

        log.trace(marker, format, arg1);

        LoggerAdapter.removeAppender(a);
        LoggerAdapter.sync();
        String out = sb.toString();

        assertTrue(out.contains(message));
        assertTrue(out.contains(arg1));
    }

    @Test(groups = "unit", timeOut = timeOutMs)
    public void testTraceFormat2ArgsWithMarker() {
        StringBuilder sb = new StringBuilder();
        Appender a = new DefaultAppender(sb);
        LoggerAdapter.addAppender(a);

        log.trace(marker, format, arg1, arg2);

        LoggerAdapter.removeAppender(a);
        LoggerAdapter.sync();
        String out = sb.toString();

        assertTrue(out.contains(message));
        assertTrue(out.contains(arg1));
        assertTrue(out.contains(arg2));
    }

    @Test(groups = "unit", timeOut = timeOutMs)
    public void testTraceFormatArgArrayWithMarker() {
        StringBuilder sb = new StringBuilder();
        Appender a = new DefaultAppender(sb);
        LoggerAdapter.addAppender(a);

        log.trace(marker, format, argArray);

        LoggerAdapter.removeAppender(a);
        LoggerAdapter.sync();
        String out = sb.toString();

        assertTrue(out.contains(message));
        assertTrue(out.contains(arg1));
        assertTrue(out.contains(arg2));
        assertTrue(out.contains(arg3));
    }

    @Test(groups = "unit", timeOut = timeOutMs)
    public void testTraceMessageThrowableWithMarker() {
        StringBuilder sb = new StringBuilder();
        Appender a = new DefaultAppender(sb);
        LoggerAdapter.addAppender(a);

        log.trace(marker, message, throwable);

        LoggerAdapter.removeAppender(a);
        LoggerAdapter.sync();
        String out = sb.toString();

        assertTrue(out.contains(message));
        assertTrue(out.contains(LoggerAdapter.stackTraceToString(throwable)));
    }

    // ///////////////////////////////////////////////////////////////////////////////

    @Test(groups = "unit")
    public void testDebugIsEnabled() {
        assertTrue(log.isDebugEnabled());
    }

    @Test(groups = "unit", timeOut = timeOutMs)
    public void testDebugMessage() {
        StringBuilder sb = new StringBuilder();
        Appender a = new DefaultAppender(sb);
        LoggerAdapter.addAppender(a);

        log.debug(message);

        LoggerAdapter.removeAppender(a);
        LoggerAdapter.sync();
        String out = sb.toString();

        assertTrue(out.contains(message));
    }

    @Test(groups = "unit", timeOut = timeOutMs)
    public void testDebugFormat1Arg() {
        StringBuilder sb = new StringBuilder();
        Appender a = new DefaultAppender(sb);
        LoggerAdapter.addAppender(a);

        log.debug(format, arg1);

        LoggerAdapter.removeAppender(a);
        LoggerAdapter.sync();
        String out = sb.toString();

        assertTrue(out.contains(message));
        assertTrue(out.contains(arg1));
    }

    @Test(groups = "unit", timeOut = timeOutMs)
    public void testDebugFormat2Args() {
        StringBuilder sb = new StringBuilder();
        Appender a = new DefaultAppender(sb);
        LoggerAdapter.addAppender(a);

        log.debug(format, arg1, arg2);

        LoggerAdapter.removeAppender(a);
        LoggerAdapter.sync();
        String out = sb.toString();

        assertTrue(out.contains(message));
        assertTrue(out.contains(arg1));
        assertTrue(out.contains(arg2));
    }

    @Test(groups = "unit", timeOut = timeOutMs)
    public void testDebugFormatArgArray() {
        StringBuilder sb = new StringBuilder();
        Appender a = new DefaultAppender(sb);
        LoggerAdapter.addAppender(a);

        log.debug(format, argArray);

        LoggerAdapter.removeAppender(a);
        LoggerAdapter.sync();
        String out = sb.toString();

        assertTrue(out.contains(message));
        assertTrue(out.contains(arg1));
        assertTrue(out.contains(arg2));
        assertTrue(out.contains(arg3));
    }

    @Test(groups = "unit", timeOut = timeOutMs)
    public void testDebugMessageThrowable() {
        StringBuilder sb = new StringBuilder();
        Appender a = new DefaultAppender(sb);
        LoggerAdapter.addAppender(a);

        log.debug(message, throwable);

        LoggerAdapter.removeAppender(a);
        LoggerAdapter.sync();
        String out = sb.toString();

        assertTrue(out.contains(message));
        assertTrue(out.contains(LoggerAdapter.stackTraceToString(throwable)));
    }

    @Test(groups = "unit")
    public void testDebugIsEnabledWithMarker() {
        assertTrue(log.isDebugEnabled(marker));
    }

    @Test(groups = "unit", timeOut = timeOutMs)
    public void testDebugMessageWithMarker() {
        StringBuilder sb = new StringBuilder();
        Appender a = new DefaultAppender(sb);
        LoggerAdapter.addAppender(a);

        log.debug(marker, message);

        LoggerAdapter.removeAppender(a);
        LoggerAdapter.sync();
        String out = sb.toString();

        assertTrue(out.contains(message));
    }

    @Test(groups = "unit", timeOut = timeOutMs)
    public void testDebugFormat1ArgWithMarker() {
        StringBuilder sb = new StringBuilder();
        Appender a = new DefaultAppender(sb);
        LoggerAdapter.addAppender(a);

        log.debug(marker, format, arg1);

        LoggerAdapter.removeAppender(a);
        LoggerAdapter.sync();
        String out = sb.toString();

        assertTrue(out.contains(message));
        assertTrue(out.contains(arg1));
    }

    @Test(groups = "unit", timeOut = timeOutMs)
    public void testDebugFormat2ArgsWithMarker() {
        StringBuilder sb = new StringBuilder();
        Appender a = new DefaultAppender(sb);
        LoggerAdapter.addAppender(a);

        log.debug(marker, format, arg1, arg2);

        LoggerAdapter.removeAppender(a);
        LoggerAdapter.sync();
        String out = sb.toString();

        assertTrue(out.contains(message));
        assertTrue(out.contains(arg1));
        assertTrue(out.contains(arg2));
    }

    @Test(groups = "unit", timeOut = timeOutMs)
    public void testDebugFormatArgArrayWithMarker() {
        StringBuilder sb = new StringBuilder();
        Appender a = new DefaultAppender(sb);
        LoggerAdapter.addAppender(a);

        log.debug(marker, format, argArray);

        LoggerAdapter.removeAppender(a);
        LoggerAdapter.sync();
        String out = sb.toString();

        assertTrue(out.contains(message));
        assertTrue(out.contains(arg1));
        assertTrue(out.contains(arg2));
        assertTrue(out.contains(arg3));
    }

    @Test(groups = "unit", timeOut = timeOutMs)
    public void testDebugMessageThrowableWithMarker() {
        StringBuilder sb = new StringBuilder();
        Appender a = new DefaultAppender(sb);
        LoggerAdapter.addAppender(a);

        log.debug(marker, message, throwable);

        LoggerAdapter.removeAppender(a);
        LoggerAdapter.sync();
        String out = sb.toString();

        assertTrue(out.contains(message));
        assertTrue(out.contains(LoggerAdapter.stackTraceToString(throwable)));
    }

    // ///////////////////////////////////////////////////////////////////////////////

    @Test(groups = "unit")
    public void testInfoIsEnabled() {
        assertTrue(log.isDebugEnabled());
    }

    @Test(groups = "unit", timeOut = timeOutMs)
    public void testInfoMessage() {
        StringBuilder sb = new StringBuilder();
        Appender a = new DefaultAppender(sb);
        LoggerAdapter.addAppender(a);

        log.info(message);

        LoggerAdapter.removeAppender(a);
        LoggerAdapter.sync();
        String out = sb.toString();

        assertTrue(out.contains(message));
    }

    @Test(groups = "unit", timeOut = timeOutMs)
    public void testInfoFormat1Arg() {
        StringBuilder sb = new StringBuilder();
        Appender a = new DefaultAppender(sb);
        LoggerAdapter.addAppender(a);

        log.info(format, arg1);

        LoggerAdapter.removeAppender(a);
        LoggerAdapter.sync();
        String out = sb.toString();

        assertTrue(out.contains(message));
        assertTrue(out.contains(arg1));
    }

    @Test(groups = "unit", timeOut = timeOutMs)
    public void testInfoFormat2Args() {
        StringBuilder sb = new StringBuilder();
        Appender a = new DefaultAppender(sb);
        LoggerAdapter.addAppender(a);

        log.info(format, arg1, arg2);

        LoggerAdapter.removeAppender(a);
        LoggerAdapter.sync();
        String out = sb.toString();

        assertTrue(out.contains(message));
        assertTrue(out.contains(arg1));
        assertTrue(out.contains(arg2));
    }

    @Test(groups = "unit", timeOut = timeOutMs)
    public void testInfoFormatArgArray() {
        StringBuilder sb = new StringBuilder();
        Appender a = new DefaultAppender(sb);
        LoggerAdapter.addAppender(a);

        log.info(format, argArray);

        LoggerAdapter.removeAppender(a);
        LoggerAdapter.sync();
        String out = sb.toString();

        assertTrue(out.contains(message));
        assertTrue(out.contains(arg1));
        assertTrue(out.contains(arg2));
        assertTrue(out.contains(arg3));
    }

    @Test(groups = "unit", timeOut = timeOutMs)
    public void testInfoMessageThrowable() {
        StringBuilder sb = new StringBuilder();
        Appender a = new DefaultAppender(sb);
        LoggerAdapter.addAppender(a);

        log.info(message, throwable);

        LoggerAdapter.removeAppender(a);
        LoggerAdapter.sync();
        String out = sb.toString();

        assertTrue(out.contains(message));
        assertTrue(out.contains(LoggerAdapter.stackTraceToString(throwable)));
    }

    @Test(groups = "unit")
    public void testInfoIsEnabledWithMarker() {
        assertTrue(log.isInfoEnabled(marker));
    }

    @Test(groups = "unit", timeOut = timeOutMs)
    public void testInfoMessageWithMarker() {
        StringBuilder sb = new StringBuilder();
        Appender a = new DefaultAppender(sb);
        LoggerAdapter.addAppender(a);

        log.info(marker, message);

        LoggerAdapter.removeAppender(a);
        LoggerAdapter.sync();
        String out = sb.toString();

        assertTrue(out.contains(message));
    }

    @Test(groups = "unit", timeOut = timeOutMs)
    public void testInfoFormat1ArgWithMarker() {
        StringBuilder sb = new StringBuilder();
        Appender a = new DefaultAppender(sb);
        LoggerAdapter.addAppender(a);

        log.info(marker, format, arg1);

        LoggerAdapter.removeAppender(a);
        LoggerAdapter.sync();
        String out = sb.toString();

        assertTrue(out.contains(message));
        assertTrue(out.contains(arg1));
    }

    @Test(groups = "unit", timeOut = timeOutMs)
    public void testInfoFormat2ArgsWithMarker() {
        StringBuilder sb = new StringBuilder();
        Appender a = new DefaultAppender(sb);
        LoggerAdapter.addAppender(a);

        log.info(marker, format, arg1, arg2);

        LoggerAdapter.removeAppender(a);
        LoggerAdapter.sync();
        String out = sb.toString();

        assertTrue(out.contains(message));
        assertTrue(out.contains(arg1));
        assertTrue(out.contains(arg2));
    }

    @Test(groups = "unit", timeOut = timeOutMs)
    public void testInfoFormatArgArrayWithMarker() {
        StringBuilder sb = new StringBuilder();
        Appender a = new DefaultAppender(sb);
        LoggerAdapter.addAppender(a);

        log.info(marker, format, argArray);

        LoggerAdapter.removeAppender(a);
        LoggerAdapter.sync();
        String out = sb.toString();

        assertTrue(out.contains(message));
        assertTrue(out.contains(arg1));
        assertTrue(out.contains(arg2));
        assertTrue(out.contains(arg3));
    }

    @Test(groups = "unit", timeOut = timeOutMs)
    public void testInfoMessageThrowableWithMarker() {
        StringBuilder sb = new StringBuilder();
        Appender a = new DefaultAppender(sb);
        LoggerAdapter.addAppender(a);

        log.info(marker, message, throwable);

        LoggerAdapter.removeAppender(a);
        LoggerAdapter.sync();
        String out = sb.toString();

        assertTrue(out.contains(message));
        assertTrue(out.contains(LoggerAdapter.stackTraceToString(throwable)));
    }

    // ///////////////////////////////////////////////////////////////////////////////

    @Test(groups = "unit")
    public void testWarnIsEnabled() {
        assertTrue(log.isWarnEnabled());
    }

    @Test(groups = "unit", timeOut = timeOutMs)
    public void testWarnMessage() {
        StringBuilder sb = new StringBuilder();
        Appender a = new DefaultAppender(sb);
        LoggerAdapter.addAppender(a);

        log.warn(message);

        LoggerAdapter.removeAppender(a);
        LoggerAdapter.sync();
        String out = sb.toString();

        assertTrue(out.contains(message));
    }

    @Test(groups = "unit", timeOut = timeOutMs)
    public void testWarnFormat1Arg() {
        StringBuilder sb = new StringBuilder();
        Appender a = new DefaultAppender(sb);
        LoggerAdapter.addAppender(a);

        log.warn(format, arg1);

        LoggerAdapter.removeAppender(a);
        LoggerAdapter.sync();
        String out = sb.toString();

        assertTrue(out.contains(message));
        assertTrue(out.contains(arg1));
    }

    @Test(groups = "unit", timeOut = timeOutMs)
    public void testWarnFormat2Args() {
        StringBuilder sb = new StringBuilder();
        Appender a = new DefaultAppender(sb);
        LoggerAdapter.addAppender(a);

        log.warn(format, arg1, arg2);

        LoggerAdapter.removeAppender(a);
        LoggerAdapter.sync();
        String out = sb.toString();

        assertTrue(out.contains(message));
        assertTrue(out.contains(arg1));
        assertTrue(out.contains(arg2));
    }

    @Test(groups = "unit", timeOut = timeOutMs)
    public void testWarnFormatArgArray() {
        StringBuilder sb = new StringBuilder();
        Appender a = new DefaultAppender(sb);
        LoggerAdapter.addAppender(a);

        log.warn(format, argArray);

        LoggerAdapter.removeAppender(a);
        LoggerAdapter.sync();
        String out = sb.toString();

        assertTrue(out.contains(message));
        assertTrue(out.contains(arg1));
        assertTrue(out.contains(arg2));
        assertTrue(out.contains(arg3));
    }

    @Test(groups = "unit", timeOut = timeOutMs)
    public void testWarnMessageThrowable() {
        StringBuilder sb = new StringBuilder();
        Appender a = new DefaultAppender(sb);
        LoggerAdapter.addAppender(a);

        log.warn(message, throwable);

        LoggerAdapter.removeAppender(a);
        LoggerAdapter.sync();
        String out = sb.toString();

        assertTrue(out.contains(message));
        assertTrue(out.contains(LoggerAdapter.stackTraceToString(throwable)));
    }

    @Test(groups = "unit")
    public void testWarnIsEnabledWithMarker() {
        assertTrue(log.isWarnEnabled(marker));
    }

    @Test(groups = "unit", timeOut = timeOutMs)
    public void testWarnMessageWithMarker() {
        StringBuilder sb = new StringBuilder();
        Appender a = new DefaultAppender(sb);
        LoggerAdapter.addAppender(a);

        log.warn(marker, message);

        LoggerAdapter.removeAppender(a);
        LoggerAdapter.sync();
        String out = sb.toString();

        assertTrue(out.contains(message));
    }

    @Test(groups = "unit", timeOut = timeOutMs)
    public void testWarnFormat1ArgWithMarker() {
        StringBuilder sb = new StringBuilder();
        Appender a = new DefaultAppender(sb);
        LoggerAdapter.addAppender(a);

        log.warn(marker, format, arg1);

        LoggerAdapter.removeAppender(a);
        LoggerAdapter.sync();
        String out = sb.toString();

        assertTrue(out.contains(message));
        assertTrue(out.contains(arg1));
    }

    @Test(groups = "unit", timeOut = timeOutMs)
    public void testWarnFormat2ArgsWithMarker() {
        StringBuilder sb = new StringBuilder();
        Appender a = new DefaultAppender(sb);
        LoggerAdapter.addAppender(a);

        log.warn(marker, format, arg1, arg2);

        LoggerAdapter.removeAppender(a);
        LoggerAdapter.sync();
        String out = sb.toString();

        assertTrue(out.contains(message));
        assertTrue(out.contains(arg1));
        assertTrue(out.contains(arg2));
    }

    @Test(groups = "unit", timeOut = timeOutMs)
    public void testWarnFormatArgArrayWithMarker() {
        StringBuilder sb = new StringBuilder();
        Appender a = new DefaultAppender(sb);
        LoggerAdapter.addAppender(a);

        log.warn(marker, format, argArray);

        LoggerAdapter.removeAppender(a);
        LoggerAdapter.sync();
        String out = sb.toString();

        assertTrue(out.contains(message));
        assertTrue(out.contains(arg1));
        assertTrue(out.contains(arg2));
        assertTrue(out.contains(arg3));
    }

    @Test(groups = "unit", timeOut = timeOutMs)
    public void testWarnMessageThrowableWithMarker() {
        StringBuilder sb = new StringBuilder();
        Appender a = new DefaultAppender(sb);
        LoggerAdapter.addAppender(a);

        log.warn(marker, message, throwable);

        LoggerAdapter.removeAppender(a);
        LoggerAdapter.sync();
        String out = sb.toString();

        assertTrue(out.contains(message));
        assertTrue(out.contains(LoggerAdapter.stackTraceToString(throwable)));
    }

    // ///////////////////////////////////////////////////////////////////////////////

    @Test(groups = "unit")
    public void testErrorIsEnabled() {
        assertTrue(log.isErrorEnabled());
    }

    @Test(groups = "unit", timeOut = timeOutMs)
    public void testErrorMessage() {
        StringBuilder sb = new StringBuilder();
        Appender a = new DefaultAppender(sb);
        LoggerAdapter.addAppender(a);

        log.error(message);

        LoggerAdapter.removeAppender(a);
        LoggerAdapter.sync();
        String out = sb.toString();

        assertTrue(out.contains(message));
    }

    @Test(groups = "unit", timeOut = timeOutMs)
    public void testErrorFormat1Arg() {
        StringBuilder sb = new StringBuilder();
        Appender a = new DefaultAppender(sb);
        LoggerAdapter.addAppender(a);

        log.error(format, arg1);

        LoggerAdapter.removeAppender(a);
        LoggerAdapter.sync();
        String out = sb.toString();

        assertTrue(out.contains(message));
        assertTrue(out.contains(arg1));
    }

    @Test(groups = "unit", timeOut = timeOutMs)
    public void testErrorFormat2Args() {
        StringBuilder sb = new StringBuilder();
        Appender a = new DefaultAppender(sb);
        LoggerAdapter.addAppender(a);

        log.error(format, arg1, arg2);

        LoggerAdapter.removeAppender(a);
        LoggerAdapter.sync();
        String out = sb.toString();

        assertTrue(out.contains(message));
        assertTrue(out.contains(arg1));
        assertTrue(out.contains(arg2));
    }

    @Test(groups = "unit", timeOut = timeOutMs)
    public void testErrorFormatArgArray() {
        StringBuilder sb = new StringBuilder();
        Appender a = new DefaultAppender(sb);
        LoggerAdapter.addAppender(a);

        log.error(format, argArray);

        LoggerAdapter.removeAppender(a);
        LoggerAdapter.sync();
        String out = sb.toString();

        assertTrue(out.contains(message));
        assertTrue(out.contains(arg1));
        assertTrue(out.contains(arg2));
        assertTrue(out.contains(arg3));
    }

    @Test(groups = "unit", timeOut = timeOutMs)
    public void testErrorMessageThrowable() {
        StringBuilder sb = new StringBuilder();
        Appender a = new DefaultAppender(sb);
        LoggerAdapter.addAppender(a);

        log.error(message, throwable);

        LoggerAdapter.removeAppender(a);
        LoggerAdapter.sync();
        String out = sb.toString();

        assertTrue(out.contains(message));
        assertTrue(out.contains(LoggerAdapter.stackTraceToString(throwable)));
    }

    @Test(groups = "unit")
    public void testErrorIsEnabledWithMarker() {
        assertTrue(log.isErrorEnabled(marker));
    }

    @Test(groups = "unit", timeOut = timeOutMs)
    public void testErrorMessageWithMarker() {
        StringBuilder sb = new StringBuilder();
        Appender a = new DefaultAppender(sb);
        LoggerAdapter.addAppender(a);

        log.error(marker, message);

        LoggerAdapter.removeAppender(a);
        LoggerAdapter.sync();
        String out = sb.toString();

        assertTrue(out.contains(message));
    }

    @Test(groups = "unit", timeOut = timeOutMs)
    public void testErrorFormat1ArgWithMarker() {
        StringBuilder sb = new StringBuilder();
        Appender a = new DefaultAppender(sb);
        LoggerAdapter.addAppender(a);

        log.error(marker, format, arg1);

        LoggerAdapter.removeAppender(a);
        LoggerAdapter.sync();
        String out = sb.toString();

        assertTrue(out.contains(message));
        assertTrue(out.contains(arg1));
    }

    @Test(groups = "unit", timeOut = timeOutMs)
    public void testErrorFormat2ArgsWithMarker() {
        StringBuilder sb = new StringBuilder();
        Appender a = new DefaultAppender(sb);
        LoggerAdapter.addAppender(a);

        log.error(marker, format, arg1, arg2);

        LoggerAdapter.removeAppender(a);
        LoggerAdapter.sync();
        String out = sb.toString();

        assertTrue(out.contains(message));
        assertTrue(out.contains(arg1));
        assertTrue(out.contains(arg2));
    }

    @Test(groups = "unit", timeOut = timeOutMs)
    public void testErrorFormatArgArrayWithMarker() {
        StringBuilder sb = new StringBuilder();
        Appender a = new DefaultAppender(sb);
        LoggerAdapter.addAppender(a);

        log.error(marker, format, argArray);

        LoggerAdapter.removeAppender(a);
        LoggerAdapter.sync();
        String out = sb.toString();

        assertTrue(out.contains(message));
        assertTrue(out.contains(arg1));
        assertTrue(out.contains(arg2));
        assertTrue(out.contains(arg3));
    }

    @Test(groups = "unit", timeOut = timeOutMs)
    public void testErrorMessageThrowableWithMarker() {
        StringBuilder sb = new StringBuilder();
        Appender a = new DefaultAppender(sb);
        LoggerAdapter.addAppender(a);

        log.error(marker, message, throwable);

        LoggerAdapter.removeAppender(a);
        LoggerAdapter.sync();
        String out = sb.toString();

        assertTrue(out.contains(message));
        assertTrue(out.contains(LoggerAdapter.stackTraceToString(throwable)));
    }
}
