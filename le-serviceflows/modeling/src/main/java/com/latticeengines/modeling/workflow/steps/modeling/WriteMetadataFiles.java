package com.latticeengines.modeling.workflow.steps.modeling;

import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import com.latticeengines.domain.exposed.exception.LedpCode;
import com.latticeengines.domain.exposed.exception.LedpException;
import com.latticeengines.domain.exposed.metadata.Attribute;
import com.latticeengines.domain.exposed.metadata.LogicalDataType;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.serviceflows.modeling.steps.ModelStepConfiguration;
import com.latticeengines.modeling.workflow.steps.modeling.ModelingServiceExecutor;

@Component("writeMetadataFiles")
public class WriteMetadataFiles extends BaseModelStep<ModelStepConfiguration> {

    private static final Logger log = LoggerFactory.getLogger(WriteMetadataFiles.class);

    @Override
    public void execute() {
        log.info("Inside WriteMetadataFiles execute()");

        Table eventTable = getEventTable();
        List<Attribute> events = eventTable.getAttributes(LogicalDataType.Event);
        for (Attribute event : events) {
            try {
                ModelingServiceExecutor modelExecutor = createModelingServiceExecutor(eventTable, event);
                modelExecutor.writeMetadataFiles();
            } catch (LedpException e) {
                throw e;
            } catch (Exception e) {
                throw new LedpException(LedpCode.LEDP_28007, e, new String[] { eventTable.getName() });
            }
        }
    }

}
