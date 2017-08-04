package com.latticeengines.eai.yarn.runtime;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.item.ItemProcessor;

import com.latticeengines.domain.exposed.eai.EaiJobConfiguration;
import com.latticeengines.eai.runtime.service.EaiRuntimeService;
import com.latticeengines.yarn.exposed.runtime.SingleContainerYarnProcessor;

public class EaiProcessor extends SingleContainerYarnProcessor<EaiJobConfiguration>
        implements ItemProcessor<EaiJobConfiguration, String> {

    private static final Logger log = LoggerFactory.getLogger(EaiProcessor.class);

    @SuppressWarnings({ "rawtypes", "unchecked" })
    @Override
    public String process(EaiJobConfiguration eaiJobConfig) throws Exception {
        log.info(String.format("Job configuration class: %s", eaiJobConfig.getClass().getName()));
        EaiRuntimeService eaiRuntimeService = EaiRuntimeService.getRunTimeService(eaiJobConfig.getClass());
        if (eaiRuntimeService == null) {
            throw new RuntimeException(
                    String.format("Cannot find the eai job service for job config class: %s", eaiJobConfig.getClass()));
        }
        eaiRuntimeService.setProgressReporter(progress -> {
            setProgress((Float) progress);
            return null;
        });
        // eaiRuntimeService.initailize(eaiJobConfig);
        eaiRuntimeService.invoke(eaiJobConfig);
        // eaiRuntimeService.finalize(eaiJobConfig);
        return null;
    }
}
