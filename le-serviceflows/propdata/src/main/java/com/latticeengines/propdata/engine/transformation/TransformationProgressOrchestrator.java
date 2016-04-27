package com.latticeengines.propdata.engine.transformation;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import com.latticeengines.propdata.engine.transformation.service.SourceTransformationService;

@Component("transformationProgressOrchestrator")
public class TransformationProgressOrchestrator {
    @Value("${propdata.job.schedule.dryrun:true}")
    private Boolean dryrun;

    @Autowired
    private SourceTransformationService sourceTransformationService;

    private String DEFAULT_PODID = "";

    public synchronized void executeRefresh() {
        if (!dryrun) {
            sourceTransformationService.scan(DEFAULT_PODID);
        }
    }
}
