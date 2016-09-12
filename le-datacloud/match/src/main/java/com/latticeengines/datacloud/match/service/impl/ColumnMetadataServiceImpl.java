package com.latticeengines.datacloud.match.service.impl;

import javax.annotation.Resource;

import org.apache.avro.Schema;
import org.apache.commons.lang.StringUtils;
import org.springframework.stereotype.Component;

import com.latticeengines.common.exposed.util.AvroUtils;
import com.latticeengines.datacloud.match.exposed.service.MetadataColumnService;
import com.latticeengines.domain.exposed.datacloud.manage.ExternalColumn;

@Component("columnMetadataService")
public class ColumnMetadataServiceImpl extends BaseColumnMetadataServiceImpl<ExternalColumn> {

    @Resource(name = "externalColumnService")
    private MetadataColumnService<ExternalColumn> externalColumnService;

    @Override
    protected MetadataColumnService<ExternalColumn> getMetadataColumnService() {
        return externalColumnService;
    }

    @Override
    protected Schema.Type getAvroTypeDataType(String dataType) {
        if (StringUtils.isEmpty(dataType)) {
            return null;
        }
        if (dataType.toLowerCase().contains("varchar")) {
            return AvroUtils.getAvroType(String.class);
        }
        if ("INT".equalsIgnoreCase(dataType)) {
            return AvroUtils.getAvroType(Integer.class);
        }
        if ("BIGINT".equalsIgnoreCase(dataType)) {
            return AvroUtils.getAvroType(Long.class);
        }
        if ("REAL".equalsIgnoreCase(dataType)) {
            return AvroUtils.getAvroType(Float.class);
        }
        if ("FLOAT".equalsIgnoreCase(dataType)) {
            return AvroUtils.getAvroType(Double.class);
        }
        if ("BIT".equalsIgnoreCase(dataType)) {
            return AvroUtils.getAvroType(Boolean.class);
        }
        if ("DATETIME".equalsIgnoreCase(dataType)) {
            return AvroUtils.getAvroType(Long.class);
        }
        if ("DATETIME2".equalsIgnoreCase(dataType)) {
            return AvroUtils.getAvroType(Long.class);
        }
        if ("DATE".equalsIgnoreCase(dataType)) {
            return AvroUtils.getAvroType(Long.class);
        }
        throw new RuntimeException("Unknown avro type for sql server data type " + dataType);
    }
}
