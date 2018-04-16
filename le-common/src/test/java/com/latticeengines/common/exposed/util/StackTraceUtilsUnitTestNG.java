package com.latticeengines.common.exposed.util;

import java.util.List;

import org.testng.Assert;
import org.testng.annotations.Test;

public class StackTraceUtilsUnitTestNG {

    @Test(groups = { "unit", "functional" })
    public void testCurrentStackTrace() {
        List<String> stack = StackTraceUtils.getCurrentStackTrace();
        Assert.assertTrue(stack.parallelStream().allMatch(st -> st.trim().startsWith(StackTraceUtils.LE_PATTERN)));
    }
    
    @Test(groups = { "unit", "functional" })
    public void testCurrentStackTraceLatticeOnly() {
        List<String> stack = StackTraceUtils.getCurrentStackTrace(true);
        Assert.assertTrue(stack.parallelStream().allMatch(st -> st.trim().startsWith(StackTraceUtils.LE_PATTERN)));
    }
    
    @Test(groups = { "unit", "functional" })
    public void testCurrentStackTraceFull() {
        List<String> stack = StackTraceUtils.getCurrentStackTrace(false);
        Assert.assertFalse(stack.parallelStream().allMatch(st -> st.trim().startsWith(StackTraceUtils.LE_PATTERN)));
    }
}
