package com.latticeengines.pls.service.impl;

import org.springframework.stereotype.Component;

import com.latticeengines.domain.exposed.metadata.MetadataSegment;
import com.latticeengines.domain.exposed.pls.ModelSummary;
import com.latticeengines.pls.service.SegmentBuilder;

@Component("modelSegmentBuilder")
public class ModelSegmentBuilder extends SegmentBuilder<ModelSummary> {

    @Override
    public MetadataSegment createSegment(String customerSpace, String name, ModelSummary source) {
        return super.createSegmentFromTable(customerSpace, name, source.getEventTableName());
    }

}
