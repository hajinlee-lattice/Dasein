package com.latticeengines.modeling.workflow.steps.modeling;

import java.util.HashMap;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.exception.LedpCode;
import com.latticeengines.domain.exposed.exception.LedpException;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.serviceflows.modeling.steps.ModelStepConfiguration;
import com.latticeengines.domain.exposed.workflow.WorkflowContextConstants;

@Component("eventCounter")
@Scope(ConfigurableBeanFactory.SCOPE_PROTOTYPE)
public class EventCounter extends BaseModelStep<ModelStepConfiguration> {

    private static final Logger log = LoggerFactory.getLogger(EventCounter.class);

    @Override
    public void execute() {
        log.info("Inside EventCounter execute()");
        Table eventTable = getEventTable();
        try {
            Map<String, Long> counterGroupResultMap = eventCounter(eventTable);
            log.info(String.format("counterGroupResultMap = %s", JsonUtils.serialize(counterGroupResultMap)));
            putObjectInContext(EVENT_COUNTER_MAP, counterGroupResultMap);
        } catch (Exception e) {
            throw new LedpException(LedpCode.LEDP_28030, e, new String[] { eventTable.getName() });
        }

        // TODO - remove this
        putStringValueInContext(EXPORT_TABLE_NAME, eventTable.getName());
        String inputPath = configuration.getModelingServiceHdfsBaseDir() + configuration.getCustomerSpace() + "/data/"
                + eventTable.getName() + "/samples";
        putStringValueInContext(EXPORT_INPUT_PATH, inputPath);

        String outputPath = configuration.getModelingServiceHdfsBaseDir() + configuration.getCustomerSpace() + "/data/"
                + eventTable.getName() + "/csv_files/postMatchEventTable";
        putStringValueInContext(EXPORT_OUTPUT_PATH, outputPath);
        saveOutputValue(WorkflowContextConstants.Outputs.POST_MATCH_EVENT_TABLE_EXPORT_PATH,
                getStringValueFromContext(EXPORT_OUTPUT_PATH));
    }

    private Map<String, Long> eventCounter(Table eventTable) throws Exception {
        Map<String, Long> counterGroupResultMap = new HashMap<>();
        ModelingServiceExecutor.Builder bldr = createModelingServiceExecutorBuilder(configuration, eventTable);
        bldr.counterGroupResultMap(counterGroupResultMap);
        ModelingServiceExecutor modelExecutor = new ModelingServiceExecutor(bldr);
        modelExecutor.eventCounter();
        return counterGroupResultMap;
    }

}
