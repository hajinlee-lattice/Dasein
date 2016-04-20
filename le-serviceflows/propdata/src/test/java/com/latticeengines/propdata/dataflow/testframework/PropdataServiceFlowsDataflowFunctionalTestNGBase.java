package com.latticeengines.propdata.dataflow.testframework;

import java.nio.file.Paths;

import org.springframework.test.context.ContextConfiguration;

import com.latticeengines.serviceflows.functionalframework.ServiceFlowsDataFlowFunctionalTestNGBase;

@ContextConfiguration(locations = { "classpath:serviceflows-core-context.xml", "classpath:serviceflows-propdata-dataflow-context.xml" })
public abstract class PropdataServiceFlowsDataflowFunctionalTestNGBase extends ServiceFlowsDataFlowFunctionalTestNGBase {

    protected static final String SHARED_AVRO_INPUT = "dataflow";

    @Override
    protected String getSharedInputDir() {
        return Paths.get(SHARED_AVRO_INPUT).toString();
    }

}
