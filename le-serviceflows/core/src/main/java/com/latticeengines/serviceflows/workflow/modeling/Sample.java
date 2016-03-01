package com.latticeengines.serviceflows.workflow.modeling;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.exception.LedpCode;
import com.latticeengines.domain.exposed.exception.LedpException;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.proxy.exposed.metadata.MetadataProxy;
import com.latticeengines.serviceflows.workflow.core.BaseWorkflowStep;
import com.latticeengines.serviceflows.workflow.core.ModelingServiceExecutor;

@Component("sample")
public class Sample extends BaseWorkflowStep<ModelStepConfiguration> {

    private static final Log log = LogFactory.getLog(Sample.class);

    @Autowired
    private MetadataProxy metadataProxy;

    @Override
    public void execute() {
        log.info("Inside Sample execute()");

        Table eventTable = getEventTable();
        try {
            sample(eventTable);
        } catch (Exception e) {
            throw new LedpException(LedpCode.LEDP_28006, e, new String[] { eventTable.getName() });
        }
    }

    private Table getEventTable() {
        if (configuration.getEventTableName() != null) {
            return metadataProxy.getTable(configuration.getCustomerSpace().toString(),
                    configuration.getEventTableName());
        } else {
            return JsonUtils.deserialize(executionContext.getString(EVENT_TABLE), Table.class);
        }
    }

    private void sample(Table eventTable) throws Exception {
        ModelingServiceExecutor.Builder bldr = createModelingServiceExecutorBuilder(configuration, eventTable);
        ModelingServiceExecutor modelExecutor = new ModelingServiceExecutor(bldr);
        modelExecutor.sample();
    }

}
