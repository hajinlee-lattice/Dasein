package com.latticeengines.dataflow.service.impl;

import java.util.ArrayList;
import java.util.List;

import org.apache.commons.lang.StringUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationContext;
import org.springframework.stereotype.Component;

import com.latticeengines.dataflow.exposed.builder.DataFlowBuilder;
import com.latticeengines.dataflow.exposed.exception.DataFlowCode;
import com.latticeengines.dataflow.exposed.exception.DataFlowException;
import com.latticeengines.dataflow.exposed.service.DataTransformationService;
import com.latticeengines.domain.exposed.dataflow.DataFlowContext;

@Component("dataTransformationService")
public class DataTransformationServiceImpl implements DataTransformationService {

    @Autowired
    private ApplicationContext appContext;

    @Override
    public void executeNamedTransformation(DataFlowContext dataFlowCtx, String dataFlowBldrBeanName) {
        validateParameters(dataFlowCtx, //
                "SOURCES", //
                "QUEUE", //
                "TARGETPATH", //
                "CUSTOMER", //
                "FLOWNAME", //
                "CHECKPOINT");

        Object dataFlowBldrBean = appContext.getBean(dataFlowBldrBeanName);

        if (!(dataFlowBldrBean instanceof DataFlowBuilder)) {
            throw new DataFlowException(DataFlowCode.DF_00000, new String[] { dataFlowBldrBeanName });
        }

        DataFlowBuilder dataFlow = (DataFlowBuilder) dataFlowBldrBean;

        boolean doCheckpoint = dataFlowCtx.getProperty("CHECKPOINT", Boolean.class);
        dataFlow.setCheckpoint(doCheckpoint);
        dataFlow.runFlow(dataFlowCtx);
    }

    private void validateParameters(DataFlowContext dataFlowCtx, String... keys) {
        List<String> missingProps = new ArrayList<>();
        for (String key : keys) {
            if (!dataFlowCtx.containsProperty(key)) {
                missingProps.add(key);
            }
        }

        if (missingProps.size() > 0) {
            throw new DataFlowException(DataFlowCode.DF_10000, new String[] { StringUtils.join(missingProps, ", ") });
        }
    }

}
