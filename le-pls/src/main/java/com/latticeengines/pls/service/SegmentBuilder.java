package com.latticeengines.pls.service;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.latticeengines.domain.exposed.metadata.MetadataSegment;
import com.latticeengines.proxy.exposed.metadata.MetadataProxy;

@Component
public abstract class SegmentBuilder<T> {
    
    @Autowired
    protected MetadataProxy metadataProxy;

    public abstract MetadataSegment createSegment(String customerSpace, String name, T source);
    
    public MetadataSegment createSegmentFromTable(String customerSpace, String name, String tableName) {
        return metadataProxy.createMetadataSegment(customerSpace, name, tableName);
    }

}
