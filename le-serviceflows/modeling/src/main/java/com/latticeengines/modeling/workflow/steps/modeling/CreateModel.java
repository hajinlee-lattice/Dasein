package com.latticeengines.modeling.workflow.steps.modeling;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

import com.latticeengines.domain.exposed.exception.LedpCode;
import com.latticeengines.domain.exposed.exception.LedpException;
import com.latticeengines.domain.exposed.metadata.Attribute;
import com.latticeengines.domain.exposed.metadata.LogicalDataType;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.serviceflows.modeling.steps.ModelStepConfiguration;
import com.latticeengines.workflow.exposed.build.InternalResourceRestApiProxy;

@Component("createModel")
@Scope(ConfigurableBeanFactory.SCOPE_PROTOTYPE)
public class CreateModel extends BaseModelStep<ModelStepConfiguration> {

    private static final Logger log = LoggerFactory.getLogger(CreateModel.class);

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
