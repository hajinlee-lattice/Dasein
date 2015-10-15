package com.latticeengines.dataflow.service.impl;

import java.util.ArrayList;
import java.util.List;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationContext;
import org.springframework.stereotype.Component;

import com.latticeengines.dataflow.exposed.builder.DataFlowBuilder;
import com.latticeengines.dataflow.exposed.service.DataTransformationService;
import com.latticeengines.domain.exposed.dataflow.DataFlowContext;
import com.latticeengines.domain.exposed.exception.LedpCode;
import com.latticeengines.domain.exposed.exception.LedpException;
import com.latticeengines.domain.exposed.metadata.Table;

@Component("dataTransformationService")
public class DataTransformationServiceImpl implements DataTransformationService {

    @Autowired
    private ApplicationContext appContext;

    @Override
    public Table executeNamedTransformation(DataFlowContext dataFlowCtx, String dataFlowBldrBeanName) {
        validateParameters(dataFlowCtx, //
                "TARGETTABLENAME",
                "QUEUE", //
                "TARGETPATH", //
                "CUSTOMER", //
                "FLOWNAME", //
                "CHECKPOINT");
        
        ApplicationContext ctx = dataFlowCtx.getProperty("APPCTX", ApplicationContext.class);
        
        if (ctx != null) {
            appContext = ctx;
        }

        Object dataFlowBldrBean = appContext.getBean(dataFlowBldrBeanName);

        if (!(dataFlowBldrBean instanceof DataFlowBuilder)) {
            throw new LedpException(LedpCode.LEDP_26000, new String[] { dataFlowBldrBeanName });
        }

        DataFlowBuilder dataFlow = (DataFlowBuilder) dataFlowBldrBean;

        boolean doCheckpoint = dataFlowCtx.getProperty("CHECKPOINT", Boolean.class);

        Configuration hadoopConfig = dataFlowCtx.getProperty("HADOOPCONF", Configuration.class);
        // Ensure that we use the fatjars rather than the hadoop class path to resolve dependencies.
        // This should eventually be set globally.
        hadoopConfig.set("mapreduce.job.user.classpath.first", "true");
        dataFlow.setLocal(hadoopConfig == null || hadoopConfig.get("fs.defaultFS").equals("file:///"));
        dataFlow.setCheckpoint(doCheckpoint);
        return dataFlow.runFlow(dataFlowCtx);
    }

    private void validateParameters(DataFlowContext dataFlowCtx, String... keys) {
        List<String> missingProps = new ArrayList<>();
        for (String key : keys) {
            if (!dataFlowCtx.containsProperty(key)) {
                missingProps.add(key);
            }
        }

        if (missingProps.size() > 0) {
            throw new LedpException(LedpCode.LEDP_26001, new String[] { StringUtils.join(missingProps, ", ") });
        }
        
    }

}
