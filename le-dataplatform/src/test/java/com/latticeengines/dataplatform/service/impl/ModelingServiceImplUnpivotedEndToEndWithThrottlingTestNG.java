package com.latticeengines.dataplatform.service.impl;

import org.springframework.test.context.ContextConfiguration;

/**
 * This is an end-to-end test with quartz job on to throttle long hanging jobs
 * 
 */
@ContextConfiguration(locations = { "classpath:dataplatform-quartz-context.xml" })
public class ModelingServiceImplUnpivotedEndToEndWithThrottlingTestNG extends
        ModelingServiceImplUnpivotedEndToEndTestNG {

}
