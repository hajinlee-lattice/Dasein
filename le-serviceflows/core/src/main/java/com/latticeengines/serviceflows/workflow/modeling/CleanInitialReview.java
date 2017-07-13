package com.latticeengines.serviceflows.workflow.modeling;

import java.util.List;

import com.latticeengines.domain.exposed.serviceflows.core.steps.ModelStepConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import com.latticeengines.domain.exposed.exception.LedpCode;
import com.latticeengines.domain.exposed.exception.LedpException;
import com.latticeengines.domain.exposed.metadata.Attribute;
import com.latticeengines.domain.exposed.metadata.LogicalDataType;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.serviceflows.workflow.core.ModelingServiceExecutor;

@Component("cleanInitialReview")
public class CleanInitialReview extends BaseModelStep<ModelStepConfiguration> {

    private static final Logger log = LoggerFactory.getLogger(CleanInitialReview.class);

    @Override
    public void execute() {
        log.info("Inside CleanInitialReview execute()");

        Table eventTable = getEventTable();
        List<Attribute> events = eventTable.getAttributes(LogicalDataType.Event);
        for (Attribute event : events) {
            try {
                ModelingServiceExecutor modelExecutor = createModelingServiceExecutor(eventTable, event);
                modelExecutor.cleanCustomerDataDir();
            } catch (LedpException e) {
                throw e;
            } catch (Exception e) {
                throw new LedpException(LedpCode.LEDP_28007, e, new String[] { eventTable.getName() });
            }
        }
    }

}
