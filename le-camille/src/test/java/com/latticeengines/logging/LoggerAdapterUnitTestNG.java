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
    public void testTrace1() {
    	Assert.assertTrue(log.isTraceEnabled());
    }
	
    @Test(groups = "unit")
    public void testTrace2() {
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
    public void testTrace3() {
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
    public void testTrace4() {
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
    public void testTrace5() {
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
    public void testTrace6() {
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
    public void testTrace7() {
    	Assert.assertTrue(log.isTraceEnabled(marker));
    }
    
    @Test(groups = "unit")
    public void testTrace8() {
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
    public void testTrace9() {
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
    public void testTrace10() {
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
    public void testTrace11() {
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
    public void testTrace12() {
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
    public void testDebug1() {
    	Assert.assertTrue(log.isDebugEnabled());
    }
	
    @Test(groups = "unit")
    public void testDebug2() {
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
    public void testDebug3() {
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
    public void testDebug4() {
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
    public void testDebug5() {
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
    public void testDebug6() {
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
    public void testDebug7() {
    	Assert.assertTrue(log.isDebugEnabled(marker));
    }
    
    @Test(groups = "unit")
    public void testDebug8() {
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
    public void testDebug9() {
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
    public void testDebug10() {
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
    public void testDebug11() {
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
    public void testDebug12() {
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
    public void testInfo1() {
    	Assert.assertTrue(log.isDebugEnabled());
    }
	
    @Test(groups = "unit")
    public void testInfo2() {
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
    public void testInfo3() {
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
    public void testInfo4() {
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
    public void testInfo5() {
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
    public void testInfo6() {
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
    public void testInfo7() {
    	Assert.assertTrue(log.isInfoEnabled(marker));
    }
    
    @Test(groups = "unit")
    public void testInfo8() {
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
    public void testInfo9() {
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
    public void testInfo10() {
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
    public void testInfo11() {
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
    public void testInfo12() {
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
    public void testWarn1() {
    	Assert.assertTrue(log.isWarnEnabled());
    }
	
    @Test(groups = "unit")
    public void testWarn2() {
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
    public void testWarn3() {
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
    public void testWarn4() {
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
    public void testWarn5() {
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
    public void testWarn6() {
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
    public void testWarn7() {
    	Assert.assertTrue(log.isWarnEnabled(marker));
    }
    
    @Test(groups = "unit")
    public void testWarn8() {
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
    public void testWarn9() {
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
    public void testWarn10() {
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
    public void testWarn11() {
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
    public void testWarn12() {
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
    public void testError1() {
    	Assert.assertTrue(log.isErrorEnabled());
    }
	
    @Test(groups = "unit")
    public void testError2() {
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
    public void testError3() {
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
    public void testError4() {
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
    public void testError5() {
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
    public void testError6() {
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
    public void testError7() {
    	Assert.assertTrue(log.isErrorEnabled(marker));
    }
    
    @Test(groups = "unit")
    public void testError8() {
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
    public void testError9() {
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
    public void testError10() {
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
    public void testError11() {
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
    public void testError12() {
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
