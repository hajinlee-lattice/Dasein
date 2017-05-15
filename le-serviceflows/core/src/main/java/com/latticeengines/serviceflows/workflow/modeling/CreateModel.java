package com.latticeengines.serviceflows.workflow.modeling;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.stereotype.Component;

import com.latticeengines.domain.exposed.exception.LedpCode;
import com.latticeengines.domain.exposed.exception.LedpException;
import com.latticeengines.domain.exposed.metadata.Attribute;
import com.latticeengines.domain.exposed.metadata.LogicalDataType;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.serviceflows.workflow.core.InternalResourceRestApiProxy;
import com.latticeengines.serviceflows.workflow.core.ModelingServiceExecutor;

@Component("createModel")
public class CreateModel extends BaseModelStep<ModelStepConfiguration> {

    private static final Log log = LogFactory.getLog(CreateModel.class);

    private InternalResourceRestApiProxy proxy = null;

    @Override
    public void execute() {
        log.info("Inside CreateModel execute()");
        if (proxy == null) {
            proxy = new InternalResourceRestApiProxy(configuration.getInternalResourceHostPort());
        }

        Table eventTable = getEventTable();
        Map<String, String> modelApplicationIdToEventColumn = new HashMap<>();
        List<Attribute> events = eventTable.getAttributes(LogicalDataType.Event);

        if (events == null || events.isEmpty()) {
            throw new IllegalStateException("There is no event to create a model on top of");
        } else {
            log.info("Found " + events.size() + " from event table");
        }
        String tenantId = configuration.getCustomerSpace().toString();
        log.info(String.format("Set model summary download flag for tenant: %s", tenantId));
        proxy.setModelSummaryDownloadFlag(tenantId);

        for (Attribute event : events) {
            try {
                ModelingServiceExecutor modelExecutor = createModelingServiceExecutor(eventTable, event);
                String modelAppId = modelExecutor.model();
                log.info("Submitted a model job " + modelAppId);
                modelApplicationIdToEventColumn.put(modelAppId, event.getName());
            } catch (LedpException e) {
                throw e;
            } catch (Exception e) {
                throw new LedpException(LedpCode.LEDP_28007, e, new String[] { eventTable.getName() });
            }
        }

        putObjectInContext(MODEL_APP_IDS, modelApplicationIdToEventColumn);
    }

}
