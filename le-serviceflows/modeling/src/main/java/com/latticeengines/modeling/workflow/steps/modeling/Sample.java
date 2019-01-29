package com.latticeengines.modeling.workflow.steps.modeling;

import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.exception.LedpCode;
import com.latticeengines.domain.exposed.exception.LedpException;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.modeling.SamplingType;
import com.latticeengines.domain.exposed.serviceflows.modeling.steps.ModelStepConfiguration;
import com.latticeengines.domain.exposed.workflow.WorkflowContextConstants;

@Component("sample")
@Scope(ConfigurableBeanFactory.SCOPE_PROTOTYPE)
public class Sample extends BaseModelStep<ModelStepConfiguration> {

    private static final Logger log = LoggerFactory.getLogger(Sample.class);

    @Value("${common.sampling.type:STRATIFIED_SAMPLING}")
    private String samplingType;

    @Override
    public void execute() {
        log.info("Inside Sample execute()");

        Table eventTable = getEventTable();
        try {
            sample(eventTable);
        } catch (Exception e) {
            throw new LedpException(LedpCode.LEDP_28006, e, new String[] { eventTable.getName() });
        }
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

    private void sample(Table eventTable) throws Exception {
        Map<String, Long> counterGroupResultMap = getMapObjectFromContext(EVENT_COUNTER_MAP, String.class, Long.class);
        log.info(String.format("counterGroupResultMap = %s", JsonUtils.serialize(counterGroupResultMap)));
        ModelingServiceExecutor.Builder bldr = createModelingServiceExecutorBuilder(configuration, eventTable);
        bldr.counterGroupResultMap(counterGroupResultMap);
        bldr.samplingType(SamplingType.valueOf(samplingType));
        ModelingServiceExecutor modelExecutor = new ModelingServiceExecutor(bldr);
        modelExecutor.sample();
    }

}
