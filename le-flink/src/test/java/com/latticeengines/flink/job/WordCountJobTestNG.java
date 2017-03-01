package com.latticeengines.flink.job;

import org.testng.annotations.Test;

import com.latticeengines.flink.framework.FlinkJobTestNGBase;

public class WordCountJobTestNG extends FlinkJobTestNGBase {

    @Test(groups = "functional")
    public void test() {

    }

    @Override
    protected String getJobName() {
        return "WordCount";
    }

    @Override
    protected String getInputResource() {
        return "wiki.txt";
    }

}
