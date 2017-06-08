package com.latticeengines.datacloudapi.engine.transformation.service.impl;

import java.util.Iterator;

import org.apache.avro.generic.GenericRecord;
import org.springframework.stereotype.Component;

import com.latticeengines.common.exposed.util.AvroUtils;

@Component("avroCacheLoaderService")
public class AvroCacheLoaderServiceImpl extends BaseCacheLoaderService<GenericRecord> {

    @Override
    protected Iterator<GenericRecord> iterator(String dirPath) {
        return AvroUtils.iterator(yarnConfiguration, BaseCacheLoaderService.toAvroGlobs(dirPath));
    }

    @Override
    protected Object getFieldValue(GenericRecord record, String fieldName) {
        return record.get(fieldName);
    }

}
