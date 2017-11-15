package com.latticeengines.apps.cdl.service.impl;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import org.apache.avro.Schema.Type;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.latticeengines.apps.cdl.service.DataFeedMetadataService;
import com.latticeengines.apps.cdl.util.VdbMetadataUtils;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.eai.CSVToHdfsConfiguration;
import com.latticeengines.domain.exposed.eai.SourceType;
import com.latticeengines.domain.exposed.metadata.Attribute;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.propdata.manage.ColumnSelection;
import com.latticeengines.proxy.exposed.metadata.MetadataProxy;

@Component("csvDataFeedMetadataService")
public class CSVDataFeedMetadataServiceImpl extends DataFeedMetadataService {

    private static final Logger log = LoggerFactory.getLogger(CSVDataFeedMetadataServiceImpl.class);

    @Autowired
    private MetadataProxy metadataProxy;

    protected CSVDataFeedMetadataServiceImpl() {
        super(SourceType.FILE.getName());
    }

    @Override
    public Table getMetadata(String metadataStr) {
        CSVToHdfsConfiguration importConfig = deserializeMetadataStrToConfig(metadataStr);
        log.info("Template table name: " + importConfig.getTemplateName());
        Table metaTable = metadataProxy.getTable(importConfig.getCustomerSpace().toString(),
                importConfig.getTemplateName());
        List<ColumnSelection.Predefined> groups = new ArrayList<>();
        groups.add(ColumnSelection.Predefined.TalkingPoint);
        for (Attribute attribute : metaTable.getAttributes()) {
            attribute.setGroupsViaList(groups);
        }
        return metaTable;
    }

    @Override
    public Table resolveMetadata(Table original, Table schemaTable) {
        return original;
    }

    @Override
    public boolean compareMetadata(Table srcTable, Table targetTable, boolean needSameType) {
        boolean result = false;
        if (srcTable == null || targetTable == null) {
            return result;
        }
        if (needSameType) {
            if (!validateAttribute(srcTable, targetTable)) {
                throw new RuntimeException("Template table attribute type should not be changed!");
            }
        }
        if (!StringUtils.equals(srcTable.getName(), targetTable.getName())) {
            return result;
        }
        if (srcTable.getAttributes().size() != targetTable.getAttributes().size()) {
            return result;
        }
        result = true;
        HashMap<String, Attribute> srcAttrs = new HashMap<>();
        for (Attribute attr : srcTable.getAttributes()) {
            srcAttrs.put(attr.getName(), attr);
        }
        for (Attribute attr : targetTable.getAttributes()) {
            if (!srcAttrs.containsKey(attr.getName())) {
                result = false;
                break;
            }
        }
        return result;
    }

    private boolean validateAttribute(Table srcTable, Table targetTable) {
        HashMap<String, Attribute> srcAttrs = new HashMap<>();
        for (Attribute attr : srcTable.getAttributes()) {
            srcAttrs.put(attr.getName(), attr);
        }
        for (Attribute attr : targetTable.getAttributes()) {
            if (srcAttrs.containsKey(attr.getName())) {
                if (!StringUtils.equalsIgnoreCase(srcAttrs.get(attr.getName()).getPhysicalDataType(),
                        attr.getPhysicalDataType())) {
                    log.error(String.format("Field %s should have the type %s, not %s", attr.getName(),
                            srcAttrs.get(attr.getName()).getSourceLogicalDataType(), attr.getSourceLogicalDataType()));
                    return false;
                }
            }
        }
        return true;
    }

    @Override
    public CustomerSpace getCustomerSpace(String metadataStr) {
        CSVToHdfsConfiguration importConfig = deserializeMetadataStrToConfig(metadataStr);
        return importConfig.getCustomerSpace();
    }

    @Override
    public String getConnectorConfig(String metadataStr, String jobIdentifier) {
        CSVToHdfsConfiguration importConfig = deserializeMetadataStrToConfig(metadataStr);
        importConfig.setJobIdentifier(jobIdentifier);
        return JsonUtils.serialize(importConfig);
    }

    @Override
    public Type getAvroType(Attribute attribute) {
        if (attribute == null) {
            return null;
        } else {
            return Type.valueOf(attribute.getDataType().toUpperCase());
        }
    }

    @Override
    public String getFileName(String metadataStr) {
        CSVToHdfsConfiguration importConfig = deserializeMetadataStrToConfig(metadataStr);
        return importConfig.getFileName();
    }

    @Override
    public String getFileDisplayName(String metadataStr) {
        CSVToHdfsConfiguration importConfig = deserializeMetadataStrToConfig(metadataStr);
        return importConfig.getFileDisplayName();
    }

    private CSVToHdfsConfiguration deserializeMetadataStrToConfig(String metadataStr) {
        CSVToHdfsConfiguration importConfig;
        try {
            importConfig = JsonUtils.deserialize(metadataStr, CSVToHdfsConfiguration.class);
        } catch (Exception e) {
            throw new RuntimeException("Cannot deserialize CSV import metadata!");
        }
        return importConfig;
    }
}
