package com.latticeengines.pls.service.impl;

import org.springframework.stereotype.Component;

import com.latticeengines.domain.exposed.metadata.MetadataSegment;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.pls.service.SegmentBuilder;

@Component("listSegmentBuilder")
public class ListSegmentBuilder extends SegmentBuilder<Table> {

    @Override
    public MetadataSegment createSegment(String customerSpace, String name, Table source) {
        return super.createSegmentFromTable(customerSpace, name, source.getName());
    }

}
