package com.latticeengines.dataflow.service.impl;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import javax.inject.Inject;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.ApplicationContext;
import org.springframework.stereotype.Component;

import com.latticeengines.common.exposed.version.VersionManager;
import com.latticeengines.dataflow.exposed.builder.DataFlowBuilder;
import com.latticeengines.dataflow.exposed.builder.common.DataFlowProperty;
import com.latticeengines.dataflow.exposed.service.DataTransformationService;
import com.latticeengines.domain.exposed.dataflow.DataFlowContext;
import com.latticeengines.domain.exposed.exception.LedpCode;
import com.latticeengines.domain.exposed.exception.LedpException;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.scheduler.exposed.LedpQueueAssigner;
import com.latticeengines.yarn.exposed.service.EMREnvService;

@Component("dataTransformationService")
public class DataTransformationServiceImpl implements DataTransformationService {

    private static final String APPCTX = "APPCTX";

    @Inject
    private ApplicationContext appContext;

    @Inject
    private VersionManager versionManager;

    @Inject
    private EMREnvService emrEnvService;

    @Value("${dataplatform.hdfs.stack}")
    private String stackName;

    @Override
    public Table executeNamedTransformation(DataFlowContext context, DataFlowBuilder dataFlow) {
        validateParameters(context);
        overwriteYarnQueueAssignment(context);

        boolean doCheckpoint = context.getProperty(DataFlowProperty.CHECKPOINT, Boolean.class);
        Boolean debug = context.getProperty(DataFlowProperty.DEBUG, Boolean.class);
        Configuration configuration = context.getProperty(DataFlowProperty.HADOOPCONF, Configuration.class);

        // Ensure that we use the fatjars rather than the hadoop class path to
        // resolve dependencies. This should eventually be set globally.
        configuration.setBoolean("mapreduce.job.user.classpath.first", true);

        Properties properties = new Properties();
        if (context.getProperty(DataFlowProperty.JOBPROPERTIES, Properties.class) != null) {
            properties.putAll(context.getProperty(DataFlowProperty.JOBPROPERTIES, Properties.class));
        }
        properties.setProperty("mapred.mapper.new-api", "false");
        context.setProperty(DataFlowProperty.JOBPROPERTIES, properties);

        dataFlow.setLocal(configuration == null
                || configuration.get(FileSystem.FS_DEFAULT_NAME_KEY).equals(FileSystem.DEFAULT_FS));
        dataFlow.setCheckpoint(doCheckpoint);
        if (context.containsProperty(DataFlowProperty.ENFORCEGLOBALORDERING)
                && context.getProperty(DataFlowProperty.ENFORCEGLOBALORDERING, Boolean.class) == false) {
            dataFlow.setEnforceGlobalOrdering(false);
        } else {
            dataFlow.setEnforceGlobalOrdering(true);
        }
        dataFlow.setDebug(debug != null ? debug : false);

        return dataFlow.runFlow(context, versionManager.getCurrentVersionInStack(stackName));
    }

    @Override
    public Table executeNamedTransformation(DataFlowContext dataFlowCtx, String dataFlowBldrBeanName) {
        validateParameters(dataFlowCtx);
        overwriteYarnQueueAssignment(dataFlowCtx);

        ApplicationContext ctx = dataFlowCtx.getProperty(APPCTX, ApplicationContext.class);

        if (ctx != null) {
            appContext = ctx;
        }
        if (appContext == null) {
            throw new LedpException(LedpCode.LEDP_26001, "ApplicationContext", null);
        }

        Object dataFlowBldrBean = appContext.getBean(dataFlowBldrBeanName);

        if (!(dataFlowBldrBean instanceof DataFlowBuilder)) {
            throw new LedpException(LedpCode.LEDP_26000, new String[] { dataFlowBldrBeanName });
        }

        DataFlowBuilder dataFlow = (DataFlowBuilder) dataFlowBldrBean;
        return executeNamedTransformation(dataFlowCtx, dataFlow);
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

    private void overwriteYarnQueueAssignment(DataFlowContext dataFlowContext) {
        String queue = dataFlowContext.getProperty(DataFlowProperty.QUEUE, String.class);
        String translatedQueue = LedpQueueAssigner.overwriteQueueAssignment(queue, emrEnvService.getYarnQueueScheme());
        dataFlowContext.setProperty(DataFlowProperty.QUEUE, translatedQueue);
    }

    private void validateParameters(DataFlowContext dataFlowCtx) {
        validateParameters(dataFlowCtx, //
                DataFlowProperty.TARGETTABLENAME, //
                DataFlowProperty.QUEUE, //
                DataFlowProperty.TARGETPATH, //
                DataFlowProperty.CUSTOMER, //
                DataFlowProperty.FLOWNAME, //
                DataFlowProperty.CHECKPOINT);
    }
}
