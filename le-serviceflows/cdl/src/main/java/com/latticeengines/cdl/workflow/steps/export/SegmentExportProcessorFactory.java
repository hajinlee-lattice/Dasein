package com.latticeengines.cdl.workflow.steps.export;

import java.util.List;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.latticeengines.domain.exposed.pls.MetadataSegmentExportType;

@Component
public class SegmentExportProcessorFactory {

    @Autowired
    private List<SegmentExportProcessor> processors;

    public SegmentExportProcessor getProcessor(MetadataSegmentExportType type) {
        return processors.stream().filter(p -> p.accepts(type)).findFirst().get();
    }
}
