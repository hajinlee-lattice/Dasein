package com.latticeengines.logging;

import org.testng.Assert;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.Marker;
import org.slf4j.MarkerFactory;
import org.testng.annotations.Test;

public class LoggerAdapterUnitTestNG {
	private static final Logger log = LoggerFactory.getLogger(new Object(){}.getClass().getEnclosingClass());
	
	private static final String message = "LoggerAdapterUnitTestNG";
	private static final String format = message + " {} {} {}";
	private static final String arg1 = "arg1";
	private static final String arg2 = "arg2";
	private static final String arg3 = "arg3";
	private static final String[] argArray = new String[]{arg1, arg2, arg3};
	private static final Throwable throwable = new Exception("TestException");
	private static final Marker marker = MarkerFactory.getMarker("TestMarker");
	
    @Test(groups = "unit")
    public void testTraceIsEnabled() {
    	Assert.assertTrue(log.isTraceEnabled());
    }
	
    @Test(groups = "unit")
    public void testTraceMessage() {
    	StringBuilder sb = new StringBuilder();
    	Appender a = new DefaultAppender(sb);
    	LoggerAdapter.addAppender(a);
    	
    	log.trace(message);
    	
    	LoggerAdapter.removeAppender(a);
    	LoggerAdapter.sync();
    	String out = sb.toString();
    	
    	Assert.assertTrue(out.contains(message));
    }
	
    @Test(groups = "unit")
    public void testTraceFormat1Arg() {
    	StringBuilder sb = new StringBuilder();
    	Appender a = new DefaultAppender(sb);
    	LoggerAdapter.addAppender(a);
    	
    	log.trace(format, arg1);
    	
    	LoggerAdapter.removeAppender(a);
    	LoggerAdapter.sync();
    	String out = sb.toString();
    	
    	Assert.assertTrue(out.contains(message));
    	Assert.assertTrue(out.contains(arg1));
    }
	
    @Test(groups = "unit")
    public void testTraceFormat2Args() {
    	StringBuilder sb = new StringBuilder();
    	Appender a = new DefaultAppender(sb);
    	LoggerAdapter.addAppender(a);
    	
    	log.trace(format, arg1, arg2);
    	
    	LoggerAdapter.removeAppender(a);
    	LoggerAdapter.sync();
    	String out = sb.toString();
    	
    	Assert.assertTrue(out.contains(message));
    	Assert.assertTrue(out.contains(arg1));
    	Assert.assertTrue(out.contains(arg2));
    }
	
    @Test(groups = "unit")
    public void testTraceFormatArgArray() {
    	StringBuilder sb = new StringBuilder();
    	Appender a = new DefaultAppender(sb);
    	LoggerAdapter.addAppender(a);
    	
    	log.trace(format, argArray);
    	
    	LoggerAdapter.removeAppender(a);
    	LoggerAdapter.sync();
    	String out = sb.toString();
    	
    	Assert.assertTrue(out.contains(message));
    	Assert.assertTrue(out.contains(arg1));
    	Assert.assertTrue(out.contains(arg2));
    	Assert.assertTrue(out.contains(arg3));
    }
    
    @Test(groups = "unit")
    public void testTraceMessageThrowable() {
    	StringBuilder sb = new StringBuilder();
    	Appender a = new DefaultAppender(sb);
    	LoggerAdapter.addAppender(a);
    	
    	log.trace(message, throwable);
    	
    	LoggerAdapter.removeAppender(a);
    	LoggerAdapter.sync();
    	String out = sb.toString();
    	
    	Assert.assertTrue(out.contains(message));
    	Assert.assertTrue(out.contains(LoggerAdapter.stackTraceToString(throwable)));
    }
    
    @Test(groups = "unit")
    public void testTraceIsEnabledWithMarker() {
    	Assert.assertTrue(log.isTraceEnabled(marker));
    }
    
    @Test(groups = "unit")
    public void testTraceMessageWithMarker() {
    	StringBuilder sb = new StringBuilder();
    	Appender a = new DefaultAppender(sb);
    	LoggerAdapter.addAppender(a);
    	
    	log.trace(marker, message);
    	
    	LoggerAdapter.removeAppender(a);
    	LoggerAdapter.sync();
    	String out = sb.toString();
    	
    	Assert.assertTrue(out.contains(message));
    }
	
    @Test(groups = "unit")
    public void testTraceFormat1ArgWithMarker() {
    	StringBuilder sb = new StringBuilder();
    	Appender a = new DefaultAppender(sb);
    	LoggerAdapter.addAppender(a);
    	
    	log.trace(marker, format, arg1);
    	
    	LoggerAdapter.removeAppender(a);
    	LoggerAdapter.sync();
    	String out = sb.toString();
    	
    	Assert.assertTrue(out.contains(message));
    	Assert.assertTrue(out.contains(arg1));
    }
	
    @Test(groups = "unit")
    public void testTraceFormat2ArgsWithMarker() {
    	StringBuilder sb = new StringBuilder();
    	Appender a = new DefaultAppender(sb);
    	LoggerAdapter.addAppender(a);
    	
    	log.trace(marker, format, arg1, arg2);
    	
    	LoggerAdapter.removeAppender(a);
    	LoggerAdapter.sync();
    	String out = sb.toString();
    	
    	Assert.assertTrue(out.contains(message));
    	Assert.assertTrue(out.contains(arg1));
    	Assert.assertTrue(out.contains(arg2));
    }
	
    @Test(groups = "unit")
    public void testTraceFormatArgArrayWithMarker() {
    	StringBuilder sb = new StringBuilder();
    	Appender a = new DefaultAppender(sb);
    	LoggerAdapter.addAppender(a);
    	
    	log.trace(marker, format, argArray);
    	
    	LoggerAdapter.removeAppender(a);
    	LoggerAdapter.sync();
    	String out = sb.toString();
    	
    	Assert.assertTrue(out.contains(message));
    	Assert.assertTrue(out.contains(arg1));
    	Assert.assertTrue(out.contains(arg2));
    	Assert.assertTrue(out.contains(arg3));
    }
    
    @Test(groups = "unit")
    public void testTraceMessageThrowableWithMarker() {
    	StringBuilder sb = new StringBuilder();
    	Appender a = new DefaultAppender(sb);
    	LoggerAdapter.addAppender(a);
    	
    	log.trace(marker, message, throwable);
    	
    	LoggerAdapter.removeAppender(a);
    	LoggerAdapter.sync();
    	String out = sb.toString();
    	
    	Assert.assertTrue(out.contains(message));
    	Assert.assertTrue(out.contains(LoggerAdapter.stackTraceToString(throwable)));
    }
    
    /////////////////////////////////////////////////////////////////////////////////
    
    @Test(groups = "unit")
    public void testDebugIsEnabled() {
    	Assert.assertTrue(log.isDebugEnabled());
    }
	
    @Test(groups = "unit")
    public void testDebugMessage() {
    	StringBuilder sb = new StringBuilder();
    	Appender a = new DefaultAppender(sb);
    	LoggerAdapter.addAppender(a);
    	
    	log.debug(message);
    	
    	LoggerAdapter.removeAppender(a);
    	LoggerAdapter.sync();
    	String out = sb.toString();
    	
    	Assert.assertTrue(out.contains(message));
    }
	
    @Test(groups = "unit")
    public void testDebugFormat1Arg() {
    	StringBuilder sb = new StringBuilder();
    	Appender a = new DefaultAppender(sb);
    	LoggerAdapter.addAppender(a);
    	
    	log.debug(format, arg1);
    	
    	LoggerAdapter.removeAppender(a);
    	LoggerAdapter.sync();
    	String out = sb.toString();
    	
    	Assert.assertTrue(out.contains(message));
    	Assert.assertTrue(out.contains(arg1));
    }
	
    @Test(groups = "unit")
    public void testDebugFormat2Args() {
    	StringBuilder sb = new StringBuilder();
    	Appender a = new DefaultAppender(sb);
    	LoggerAdapter.addAppender(a);
    	
    	log.debug(format, arg1, arg2);
    	
    	LoggerAdapter.removeAppender(a);
    	LoggerAdapter.sync();
    	String out = sb.toString();
    	
    	Assert.assertTrue(out.contains(message));
    	Assert.assertTrue(out.contains(arg1));
    	Assert.assertTrue(out.contains(arg2));
    }
	
    @Test(groups = "unit")
    public void testDebugFormatArgArray() {
    	StringBuilder sb = new StringBuilder();
    	Appender a = new DefaultAppender(sb);
    	LoggerAdapter.addAppender(a);
    	
    	log.debug(format, argArray);
    	
    	LoggerAdapter.removeAppender(a);
    	LoggerAdapter.sync();
    	String out = sb.toString();
    	
    	Assert.assertTrue(out.contains(message));
    	Assert.assertTrue(out.contains(arg1));
    	Assert.assertTrue(out.contains(arg2));
    	Assert.assertTrue(out.contains(arg3));
    }
    
    @Test(groups = "unit")
    public void testDebugMessageThrowable() {
    	StringBuilder sb = new StringBuilder();
    	Appender a = new DefaultAppender(sb);
    	LoggerAdapter.addAppender(a);
    	
    	log.debug(message, throwable);
    	
    	LoggerAdapter.removeAppender(a);
    	LoggerAdapter.sync();
    	String out = sb.toString();
    	
    	Assert.assertTrue(out.contains(message));
    	Assert.assertTrue(out.contains(LoggerAdapter.stackTraceToString(throwable)));
    }
    
    @Test(groups = "unit")
    public void testDebugIsEnabledWithMarker() {
    	Assert.assertTrue(log.isDebugEnabled(marker));
    }
    
    @Test(groups = "unit")
    public void testDebugMessageWithMarker() {
    	StringBuilder sb = new StringBuilder();
    	Appender a = new DefaultAppender(sb);
    	LoggerAdapter.addAppender(a);
    	
    	log.debug(marker, message);
    	
    	LoggerAdapter.removeAppender(a);
    	LoggerAdapter.sync();
    	String out = sb.toString();
    	
    	Assert.assertTrue(out.contains(message));
    }
	
    @Test(groups = "unit")
    public void testDebugFormat1ArgWithMarker() {
    	StringBuilder sb = new StringBuilder();
    	Appender a = new DefaultAppender(sb);
    	LoggerAdapter.addAppender(a);
    	
    	log.debug(marker, format, arg1);
    	
    	LoggerAdapter.removeAppender(a);
    	LoggerAdapter.sync();
    	String out = sb.toString();
    	
    	Assert.assertTrue(out.contains(message));
    	Assert.assertTrue(out.contains(arg1));
    }
	
    @Test(groups = "unit")
    public void testDebugFormat2ArgsWithMarker() {
    	StringBuilder sb = new StringBuilder();
    	Appender a = new DefaultAppender(sb);
    	LoggerAdapter.addAppender(a);
    	
    	log.debug(marker, format, arg1, arg2);
    	
    	LoggerAdapter.removeAppender(a);
    	LoggerAdapter.sync();
    	String out = sb.toString();
    	
    	Assert.assertTrue(out.contains(message));
    	Assert.assertTrue(out.contains(arg1));
    	Assert.assertTrue(out.contains(arg2));
    }
	
    @Test(groups = "unit")
    public void testDebugFormatArgArrayWithMarker() {
    	StringBuilder sb = new StringBuilder();
    	Appender a = new DefaultAppender(sb);
    	LoggerAdapter.addAppender(a);
    	
    	log.debug(marker, format, argArray);
    	
    	LoggerAdapter.removeAppender(a);
    	LoggerAdapter.sync();
    	String out = sb.toString();
    	
    	Assert.assertTrue(out.contains(message));
    	Assert.assertTrue(out.contains(arg1));
    	Assert.assertTrue(out.contains(arg2));
    	Assert.assertTrue(out.contains(arg3));
    }
    
    @Test(groups = "unit")
    public void testDebugMessageThrowableWithMarker() {
    	StringBuilder sb = new StringBuilder();
    	Appender a = new DefaultAppender(sb);
    	LoggerAdapter.addAppender(a);
    	
    	log.debug(marker, message, throwable);
    	
    	LoggerAdapter.removeAppender(a);
    	LoggerAdapter.sync();
    	String out = sb.toString();
    	
    	Assert.assertTrue(out.contains(message));
    	Assert.assertTrue(out.contains(LoggerAdapter.stackTraceToString(throwable)));
    }
    
    /////////////////////////////////////////////////////////////////////////////////
    
    @Test(groups = "unit")
    public void testInfoIsEnabled() {
    	Assert.assertTrue(log.isDebugEnabled());
    }
	
    @Test(groups = "unit")
    public void testInfoMessage() {
    	StringBuilder sb = new StringBuilder();
    	Appender a = new DefaultAppender(sb);
    	LoggerAdapter.addAppender(a);
    	
    	log.info(message);
    	
    	LoggerAdapter.removeAppender(a);
    	LoggerAdapter.sync();
    	String out = sb.toString();
    	
    	Assert.assertTrue(out.contains(message));
    }
	
    @Test(groups = "unit")
    public void testInfoFormat1Arg() {
    	StringBuilder sb = new StringBuilder();
    	Appender a = new DefaultAppender(sb);
    	LoggerAdapter.addAppender(a);
    	
    	log.info(format, arg1);
    	
    	LoggerAdapter.removeAppender(a);
    	LoggerAdapter.sync();
    	String out = sb.toString();
    	
    	Assert.assertTrue(out.contains(message));
    	Assert.assertTrue(out.contains(arg1));
    }
	
    @Test(groups = "unit")
    public void testInfoFormat2Args() {
    	StringBuilder sb = new StringBuilder();
    	Appender a = new DefaultAppender(sb);
    	LoggerAdapter.addAppender(a);
    	
    	log.info(format, arg1, arg2);
    	
    	LoggerAdapter.removeAppender(a);
    	LoggerAdapter.sync();
    	String out = sb.toString();
    	
    	Assert.assertTrue(out.contains(message));
    	Assert.assertTrue(out.contains(arg1));
    	Assert.assertTrue(out.contains(arg2));
    }
	
    @Test(groups = "unit")
    public void testInfoFormatArgArray() {
    	StringBuilder sb = new StringBuilder();
    	Appender a = new DefaultAppender(sb);
    	LoggerAdapter.addAppender(a);
    	
    	log.info(format, argArray);
    	
    	LoggerAdapter.removeAppender(a);
    	LoggerAdapter.sync();
    	String out = sb.toString();
    	
    	Assert.assertTrue(out.contains(message));
    	Assert.assertTrue(out.contains(arg1));
    	Assert.assertTrue(out.contains(arg2));
    	Assert.assertTrue(out.contains(arg3));
    }
    
    @Test(groups = "unit")
    public void testInfoMessageThrowable() {
    	StringBuilder sb = new StringBuilder();
    	Appender a = new DefaultAppender(sb);
    	LoggerAdapter.addAppender(a);
    	
    	log.info(message, throwable);
    	
    	LoggerAdapter.removeAppender(a);
    	LoggerAdapter.sync();
    	String out = sb.toString();
    	
    	Assert.assertTrue(out.contains(message));
    	Assert.assertTrue(out.contains(LoggerAdapter.stackTraceToString(throwable)));
    }
    
    @Test(groups = "unit")
    public void testInfoIsEnabledWithMarker() {
    	Assert.assertTrue(log.isInfoEnabled(marker));
    }
    
    @Test(groups = "unit")
    public void testInfoMessageWithMarker() {
    	StringBuilder sb = new StringBuilder();
    	Appender a = new DefaultAppender(sb);
    	LoggerAdapter.addAppender(a);
    	
    	log.info(marker, message);
    	
    	LoggerAdapter.removeAppender(a);
    	LoggerAdapter.sync();
    	String out = sb.toString();
    	
    	Assert.assertTrue(out.contains(message));
    }
	
    @Test(groups = "unit")
    public void testInfoFormat1ArgWithMarker() {
    	StringBuilder sb = new StringBuilder();
    	Appender a = new DefaultAppender(sb);
    	LoggerAdapter.addAppender(a);
    	
    	log.info(marker, format, arg1);
    	
    	LoggerAdapter.removeAppender(a);
    	LoggerAdapter.sync();
    	String out = sb.toString();
    	
    	Assert.assertTrue(out.contains(message));
    	Assert.assertTrue(out.contains(arg1));
    }
	
    @Test(groups = "unit")
    public void testInfoFormat2ArgsWithMarker() {
    	StringBuilder sb = new StringBuilder();
    	Appender a = new DefaultAppender(sb);
    	LoggerAdapter.addAppender(a);
    	
    	log.info(marker, format, arg1, arg2);
    	
    	LoggerAdapter.removeAppender(a);
    	LoggerAdapter.sync();
    	String out = sb.toString();
    	
    	Assert.assertTrue(out.contains(message));
    	Assert.assertTrue(out.contains(arg1));
    	Assert.assertTrue(out.contains(arg2));
    }
	
    @Test(groups = "unit")
    public void testInfoFormatArgArrayWithMarker() {
    	StringBuilder sb = new StringBuilder();
    	Appender a = new DefaultAppender(sb);
    	LoggerAdapter.addAppender(a);
    	
    	log.info(marker, format, argArray);
    	
    	LoggerAdapter.removeAppender(a);
    	LoggerAdapter.sync();
    	String out = sb.toString();
    	
    	Assert.assertTrue(out.contains(message));
    	Assert.assertTrue(out.contains(arg1));
    	Assert.assertTrue(out.contains(arg2));
    	Assert.assertTrue(out.contains(arg3));
    }
    
    @Test(groups = "unit")
    public void testInfoMessageThrowableWithMarker() {
    	StringBuilder sb = new StringBuilder();
    	Appender a = new DefaultAppender(sb);
    	LoggerAdapter.addAppender(a);
    	
    	log.info(marker, message, throwable);
    	
    	LoggerAdapter.removeAppender(a);
    	LoggerAdapter.sync();
    	String out = sb.toString();
    	
    	Assert.assertTrue(out.contains(message));
    	Assert.assertTrue(out.contains(LoggerAdapter.stackTraceToString(throwable)));
    }
    
    /////////////////////////////////////////////////////////////////////////////////
    
    @Test(groups = "unit")
    public void testWarnIsEnabled() {
    	Assert.assertTrue(log.isWarnEnabled());
    }
	
    @Test(groups = "unit")
    public void testWarnMessage() {
    	StringBuilder sb = new StringBuilder();
    	Appender a = new DefaultAppender(sb);
    	LoggerAdapter.addAppender(a);
    	
    	log.warn(message);
    	
    	LoggerAdapter.removeAppender(a);
    	LoggerAdapter.sync();
    	String out = sb.toString();
    	
    	Assert.assertTrue(out.contains(message));
    }
	
    @Test(groups = "unit")
    public void testWarnFormat1Arg() {
    	StringBuilder sb = new StringBuilder();
    	Appender a = new DefaultAppender(sb);
    	LoggerAdapter.addAppender(a);
    	
    	log.warn(format, arg1);
    	
    	LoggerAdapter.removeAppender(a);
    	LoggerAdapter.sync();
    	String out = sb.toString();
    	
    	Assert.assertTrue(out.contains(message));
    	Assert.assertTrue(out.contains(arg1));
    }
	
    @Test(groups = "unit")
    public void testWarnFormat2Args() {
    	StringBuilder sb = new StringBuilder();
    	Appender a = new DefaultAppender(sb);
    	LoggerAdapter.addAppender(a);
    	
    	log.warn(format, arg1, arg2);
    	
    	LoggerAdapter.removeAppender(a);
    	LoggerAdapter.sync();
    	String out = sb.toString();
    	
    	Assert.assertTrue(out.contains(message));
    	Assert.assertTrue(out.contains(arg1));
    	Assert.assertTrue(out.contains(arg2));
    }
	
    @Test(groups = "unit")
    public void testWarnFormatArgArray() {
    	StringBuilder sb = new StringBuilder();
    	Appender a = new DefaultAppender(sb);
    	LoggerAdapter.addAppender(a);
    	
    	log.warn(format, argArray);
    	
    	LoggerAdapter.removeAppender(a);
    	LoggerAdapter.sync();
    	String out = sb.toString();
    	
    	Assert.assertTrue(out.contains(message));
    	Assert.assertTrue(out.contains(arg1));
    	Assert.assertTrue(out.contains(arg2));
    	Assert.assertTrue(out.contains(arg3));
    }
    
    @Test(groups = "unit")
    public void testWarnMessageThrowable() {
    	StringBuilder sb = new StringBuilder();
    	Appender a = new DefaultAppender(sb);
    	LoggerAdapter.addAppender(a);
    	
    	log.warn(message, throwable);
    	
    	LoggerAdapter.removeAppender(a);
    	LoggerAdapter.sync();
    	String out = sb.toString();
    	
    	Assert.assertTrue(out.contains(message));
    	Assert.assertTrue(out.contains(LoggerAdapter.stackTraceToString(throwable)));
    }
    
    @Test(groups = "unit")
    public void testWarnIsEnabledWithMarker() {
    	Assert.assertTrue(log.isWarnEnabled(marker));
    }
    
    @Test(groups = "unit")
    public void testWarnMessageWithMarker() {
    	StringBuilder sb = new StringBuilder();
    	Appender a = new DefaultAppender(sb);
    	LoggerAdapter.addAppender(a);
    	
    	log.warn(marker, message);
    	
    	LoggerAdapter.removeAppender(a);
    	LoggerAdapter.sync();
    	String out = sb.toString();
    	
    	Assert.assertTrue(out.contains(message));
    }
	
    @Test(groups = "unit")
    public void testWarnFormat1ArgWithMarker() {
    	StringBuilder sb = new StringBuilder();
    	Appender a = new DefaultAppender(sb);
    	LoggerAdapter.addAppender(a);
    	
    	log.warn(marker, format, arg1);
    	
    	LoggerAdapter.removeAppender(a);
    	LoggerAdapter.sync();
    	String out = sb.toString();
    	
    	Assert.assertTrue(out.contains(message));
    	Assert.assertTrue(out.contains(arg1));
    }
	
    @Test(groups = "unit")
    public void testWarnFormat2ArgsWithMarker() {
    	StringBuilder sb = new StringBuilder();
    	Appender a = new DefaultAppender(sb);
    	LoggerAdapter.addAppender(a);
    	
    	log.warn(marker, format, arg1, arg2);
    	
    	LoggerAdapter.removeAppender(a);
    	LoggerAdapter.sync();
    	String out = sb.toString();
    	
    	Assert.assertTrue(out.contains(message));
    	Assert.assertTrue(out.contains(arg1));
    	Assert.assertTrue(out.contains(arg2));
    }
	
    @Test(groups = "unit")
    public void testWarnFormatArgArrayWithMarker() {
    	StringBuilder sb = new StringBuilder();
    	Appender a = new DefaultAppender(sb);
    	LoggerAdapter.addAppender(a);
    	
    	log.warn(marker, format, argArray);
    	
    	LoggerAdapter.removeAppender(a);
    	LoggerAdapter.sync();
    	String out = sb.toString();
    	
    	Assert.assertTrue(out.contains(message));
    	Assert.assertTrue(out.contains(arg1));
    	Assert.assertTrue(out.contains(arg2));
    	Assert.assertTrue(out.contains(arg3));
    }
    
    @Test(groups = "unit")
    public void testWarnMessageThrowableWithMarker() {
    	StringBuilder sb = new StringBuilder();
    	Appender a = new DefaultAppender(sb);
    	LoggerAdapter.addAppender(a);
    	
    	log.warn(marker, message, throwable);
    	
    	LoggerAdapter.removeAppender(a);
    	LoggerAdapter.sync();
    	String out = sb.toString();
    	
    	Assert.assertTrue(out.contains(message));
    	Assert.assertTrue(out.contains(LoggerAdapter.stackTraceToString(throwable)));
    }
    
    /////////////////////////////////////////////////////////////////////////////////
    
    @Test(groups = "unit")
    public void testErrorIsEnabled() {
    	Assert.assertTrue(log.isErrorEnabled());
    }
	
    @Test(groups = "unit")
    public void testErrorMessage() {
    	StringBuilder sb = new StringBuilder();
    	Appender a = new DefaultAppender(sb);
    	LoggerAdapter.addAppender(a);
    	
    	log.error(message);
    	
    	LoggerAdapter.removeAppender(a);
    	LoggerAdapter.sync();
    	String out = sb.toString();
    	
    	Assert.assertTrue(out.contains(message));
    }
	
    @Test(groups = "unit")
    public void testErrorFormat1Arg() {
    	StringBuilder sb = new StringBuilder();
    	Appender a = new DefaultAppender(sb);
    	LoggerAdapter.addAppender(a);
    	
    	log.error(format, arg1);
    	
    	LoggerAdapter.removeAppender(a);
    	LoggerAdapter.sync();
    	String out = sb.toString();
    	
    	Assert.assertTrue(out.contains(message));
    	Assert.assertTrue(out.contains(arg1));
    }
	
    @Test(groups = "unit")
    public void testErrorFormat2Args() {
    	StringBuilder sb = new StringBuilder();
    	Appender a = new DefaultAppender(sb);
    	LoggerAdapter.addAppender(a);
    	
    	log.error(format, arg1, arg2);
    	
    	LoggerAdapter.removeAppender(a);
    	LoggerAdapter.sync();
    	String out = sb.toString();
    	
    	Assert.assertTrue(out.contains(message));
    	Assert.assertTrue(out.contains(arg1));
    	Assert.assertTrue(out.contains(arg2));
    }
	
    @Test(groups = "unit")
    public void testErrorFormatArgArray() {
    	StringBuilder sb = new StringBuilder();
    	Appender a = new DefaultAppender(sb);
    	LoggerAdapter.addAppender(a);
    	
    	log.error(format, argArray);
    	
    	LoggerAdapter.removeAppender(a);
    	LoggerAdapter.sync();
    	String out = sb.toString();
    	
    	Assert.assertTrue(out.contains(message));
    	Assert.assertTrue(out.contains(arg1));
    	Assert.assertTrue(out.contains(arg2));
    	Assert.assertTrue(out.contains(arg3));
    }
    
    @Test(groups = "unit")
    public void testErrorMessageThrowable() {
    	StringBuilder sb = new StringBuilder();
    	Appender a = new DefaultAppender(sb);
    	LoggerAdapter.addAppender(a);
    	
    	log.error(message, throwable);
    	
    	LoggerAdapter.removeAppender(a);
    	LoggerAdapter.sync();
    	String out = sb.toString();
    	
    	Assert.assertTrue(out.contains(message));
    	Assert.assertTrue(out.contains(LoggerAdapter.stackTraceToString(throwable)));
    }
    
    @Test(groups = "unit")
    public void testErrorIsEnabledWithMarker() {
    	Assert.assertTrue(log.isErrorEnabled(marker));
    }
    
    @Test(groups = "unit")
    public void testErrorMessageWithMarker() {
    	StringBuilder sb = new StringBuilder();
    	Appender a = new DefaultAppender(sb);
    	LoggerAdapter.addAppender(a);
    	
    	log.error(marker, message);
    	
    	LoggerAdapter.removeAppender(a);
    	LoggerAdapter.sync();
    	String out = sb.toString();
    	
    	Assert.assertTrue(out.contains(message));
    }
	
    @Test(groups = "unit")
    public void testErrorFormat1ArgWithMarker() {
    	StringBuilder sb = new StringBuilder();
    	Appender a = new DefaultAppender(sb);
    	LoggerAdapter.addAppender(a);
    	
    	log.error(marker, format, arg1);
    	
    	LoggerAdapter.removeAppender(a);
    	LoggerAdapter.sync();
    	String out = sb.toString();
    	
    	Assert.assertTrue(out.contains(message));
    	Assert.assertTrue(out.contains(arg1));
    }
	
    @Test(groups = "unit")
    public void testErrorFormat2ArgsWithMarker() {
    	StringBuilder sb = new StringBuilder();
    	Appender a = new DefaultAppender(sb);
    	LoggerAdapter.addAppender(a);
    	
    	log.error(marker, format, arg1, arg2);
    	
    	LoggerAdapter.removeAppender(a);
    	LoggerAdapter.sync();
    	String out = sb.toString();
    	
    	Assert.assertTrue(out.contains(message));
    	Assert.assertTrue(out.contains(arg1));
    	Assert.assertTrue(out.contains(arg2));
    }
	
    @Test(groups = "unit")
    public void testErrorFormatArgArrayWithMarker() {
    	StringBuilder sb = new StringBuilder();
    	Appender a = new DefaultAppender(sb);
    	LoggerAdapter.addAppender(a);
    	
    	log.error(marker, format, argArray);
    	
    	LoggerAdapter.removeAppender(a);
    	LoggerAdapter.sync();
    	String out = sb.toString();
    	
    	Assert.assertTrue(out.contains(message));
    	Assert.assertTrue(out.contains(arg1));
    	Assert.assertTrue(out.contains(arg2));
    	Assert.assertTrue(out.contains(arg3));
    }
    
    @Test(groups = "unit")
    public void testErrorMessageThrowableWithMarker() {
    	StringBuilder sb = new StringBuilder();
    	Appender a = new DefaultAppender(sb);
    	LoggerAdapter.addAppender(a);
    	
    	log.error(marker, message, throwable);
    	
    	LoggerAdapter.removeAppender(a);
    	LoggerAdapter.sync();
    	String out = sb.toString();
    	
    	Assert.assertTrue(out.contains(message));
    	Assert.assertTrue(out.contains(LoggerAdapter.stackTraceToString(throwable)));
    }
}
