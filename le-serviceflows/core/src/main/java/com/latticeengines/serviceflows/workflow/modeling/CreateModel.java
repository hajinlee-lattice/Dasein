package com.latticeengines.serviceflows.workflow.modeling;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.stereotype.Component;

import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.exception.LedpCode;
import com.latticeengines.domain.exposed.exception.LedpException;
import com.latticeengines.domain.exposed.metadata.Attribute;
import com.latticeengines.domain.exposed.metadata.LogicalDataType;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.serviceflows.workflow.core.ModelingServiceExecutor;

@Component("createModel")
public class CreateModel extends BaseModelStep<ModelStepConfiguration> {

    private static final Log log = LogFactory.getLog(CreateModel.class);

    @Override
    public void execute() {
        log.info("Inside CreateModel execute()");

        Table eventTable = getEventTable();
        Map<String, String> modelApplicationIdToEventColumn = new HashMap<>();
        List<Attribute> events = eventTable.getAttributes(LogicalDataType.Event);
        for (Attribute event : events) {
            try {
                ModelingServiceExecutor modelExecutor = createModelingServiceExecutor(eventTable, event);
                String modelAppId = modelExecutor.model();
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
