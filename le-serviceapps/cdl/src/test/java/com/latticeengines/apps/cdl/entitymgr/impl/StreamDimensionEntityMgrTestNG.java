package com.latticeengines.apps.cdl.entitymgr.impl;

import javax.inject.Inject;

import org.testng.annotations.Listeners;
import org.testng.annotations.Test;

import com.latticeengines.apps.cdl.entitymgr.DimensionEntityMgr;
import com.latticeengines.apps.cdl.entitymgr.StreamEntityMgr;
import com.latticeengines.apps.cdl.testframework.CDLFunctionalTestNGBase;
import com.latticeengines.testframework.service.impl.SimpleRetryListener;

@Listeners({ SimpleRetryListener.class })
public class StreamDimensionEntityMgrTestNG extends CDLFunctionalTestNGBase {

    @Inject
    private DimensionEntityMgr dimensionEntityMgr;

    @Inject
    private StreamEntityMgr streamEntityMgr;
    
    @Test(groups = "functional")
    public void test() {

    }

}
