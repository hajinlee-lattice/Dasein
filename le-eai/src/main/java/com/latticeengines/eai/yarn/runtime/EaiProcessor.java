package com.latticeengines.eai.yarn.runtime;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.beans.factory.annotation.Autowired;

import com.latticeengines.domain.exposed.eai.EaiJobConfiguration;
import com.latticeengines.domain.exposed.eai.ImportConfiguration;
import com.latticeengines.eai.runtime.service.EaiRuntimeService;
import com.latticeengines.yarn.exposed.runtime.SingleContainerYarnProcessor;

public class EaiProcessor extends SingleContainerYarnProcessor<EaiJobConfiguration>
        implements ItemProcessor<EaiJobConfiguration, String> {

    private static final Logger log = LoggerFactory.getLogger(EaiProcessor.class);

    @Autowired
    private ImportProcessor importProcessor;

    @SuppressWarnings({ "rawtypes", "unchecked" })
    @Override
    public String process(EaiJobConfiguration eaiJobConfig) throws Exception {
        if (eaiJobConfig instanceof ImportConfiguration) {
            log.info("Directing import job to " + importProcessor.getClass().getSimpleName());
            return importProcessor.process((ImportConfiguration) eaiJobConfig);
        }
        EaiRuntimeService eaiRuntimeService = EaiRuntimeService.getRunTimeService(eaiJobConfig.getClass());
        eaiRuntimeService.setProgressReporter(progress -> {
            setProgress((Float) progress);
            return null;
        });
        eaiRuntimeService.invoke(eaiJobConfig);
        return null;
    }
}
