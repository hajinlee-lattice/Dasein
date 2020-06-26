package com.latticeengines.spark.exposed.utils;

import org.testng.Assert;
import org.testng.annotations.Test;

import com.latticeengines.domain.exposed.spark.common.CopyConfig;
import com.latticeengines.spark.exposed.job.AbstractSparkJob;
import com.latticeengines.spark.exposed.job.common.CopyJob;

public class SparkJobClzUtilsUnitTestNG {

    @Test
    public void testFindClz() throws ClassNotFoundException {
        Class<? extends AbstractSparkJob<CopyConfig>> clz = //
                SparkJobClzUtils.findSparkJobClz("CopyJob", CopyConfig.class);
        Assert.assertEquals(clz, CopyJob.class);
    }

}
