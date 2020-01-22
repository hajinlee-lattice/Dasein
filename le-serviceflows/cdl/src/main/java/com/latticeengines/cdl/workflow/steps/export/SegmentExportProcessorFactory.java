package com.latticeengines.cdl.workflow.steps.export;

import java.util.List;

import javax.inject.Inject;

import org.springframework.stereotype.Component;

import com.latticeengines.domain.exposed.pls.AtlasExportType;

@Component
public class SegmentExportProcessorFactory {

    @Inject
    private List<SegmentExportProcessor> processors;

    public SegmentExportProcessor getProcessor(AtlasExportType type) {
        return processors.stream().filter(p -> p.accepts(type)).findFirst().get();
    }
}
