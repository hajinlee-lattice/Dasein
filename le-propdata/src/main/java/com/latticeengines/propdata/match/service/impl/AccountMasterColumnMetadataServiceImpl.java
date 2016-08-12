package com.latticeengines.propdata.match.service.impl;

import javax.annotation.Resource;

import org.apache.avro.Schema;
import org.apache.commons.lang.StringUtils;
import org.springframework.stereotype.Component;

import com.latticeengines.common.exposed.util.AvroUtils;
import com.latticeengines.domain.exposed.propdata.manage.AccountMasterColumn;
import com.latticeengines.propdata.match.service.MetadataColumnService;

@Component("accountMasterColumnMetadataService")
public class AccountMasterColumnMetadataServiceImpl extends BaseColumnMetadataServiceImpl<AccountMasterColumn> {

    @Resource(name = "accountMasterColumnService")
    private MetadataColumnService<AccountMasterColumn> accountmasterColumnService;

    @Override
    protected MetadataColumnService<AccountMasterColumn> getMetadataColumnService() {
        return accountmasterColumnService;
    }

    @Override
    protected Schema.Type getAvroTypeDataType(String dataType) {
        if (StringUtils.isEmpty(dataType)) {
            return null;
        }
        if ("INTEGER".equalsIgnoreCase(dataType)) {
            return AvroUtils.getAvroType(Integer.class);
        }
        if ("LONG".equalsIgnoreCase(dataType)) {
            return AvroUtils.getAvroType(Long.class);
        }
        if ("FLOAT".equalsIgnoreCase(dataType)) {
            return AvroUtils.getAvroType(Float.class);
        }
        if ("DOUBLE".equalsIgnoreCase(dataType)) {
            return AvroUtils.getAvroType(Double.class);
        }
        if ("BOOLEAN".equalsIgnoreCase(dataType)) {
            return AvroUtils.getAvroType(Boolean.class);
        }
        if ("DATE".equalsIgnoreCase(dataType)) {
            return AvroUtils.getAvroType(Long.class);
        }
        if ("TIMESTAMP".equalsIgnoreCase(dataType)) {
            return AvroUtils.getAvroType(Long.class);
        }
        if ("STRING".equalsIgnoreCase(dataType)) {
            return AvroUtils.getAvroType(String.class);
        }
        throw new RuntimeException("Unknown avro type for sql server data type " + dataType);
    }

}
