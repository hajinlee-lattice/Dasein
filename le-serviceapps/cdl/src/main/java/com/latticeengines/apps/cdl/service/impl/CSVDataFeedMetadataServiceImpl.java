package com.latticeengines.apps.cdl.service.impl;

import java.util.HashMap;

import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.latticeengines.apps.cdl.service.DataFeedMetadataService;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.eai.CSVToHdfsConfiguration;
import com.latticeengines.domain.exposed.eai.SourceType;
import com.latticeengines.domain.exposed.metadata.Attribute;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.pls.SchemaInterpretation;
import com.latticeengines.proxy.exposed.metadata.MetadataProxy;

@Component("csvDataFeedMetadataService")
public class CSVDataFeedMetadataServiceImpl extends DataFeedMetadataService {

    @Autowired
    private MetadataProxy metadataProxy;

    protected CSVDataFeedMetadataServiceImpl() {
        super(SourceType.FILE.getName());
    }

    @Override
    public Table getMetadata(String metadataStr) {
        CSVToHdfsConfiguration importConfig;
        try {
            importConfig = JsonUtils.deserialize(metadataStr, CSVToHdfsConfiguration.class);
        } catch (Exception e) {
            throw new RuntimeException("Cannot deserialize CSV import metadata!");
        }
        return metadataProxy.getTable(importConfig.getCustomerSpace().toString(), importConfig.getTemplateName());
    }

    @Override
    public Table resolveMetadata(Table original, SchemaInterpretation schemaInterpretation) {
        return original;
    }

    @Override
    public boolean compareMetadata(Table srcTable, Table targetTable, boolean needSameType) {
        boolean result = false;
        if (srcTable == null || targetTable == null) {
            return result;
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
            srcAttrs.put(attr.getName(),attr);
        }
        for (Attribute attr : targetTable.getAttributes()) {
            if (srcAttrs.containsKey(attr.getName())) {
                if (!StringUtils.equals(srcAttrs.get(attr.getName()).getSourceLogicalDataType(),
                        attr.getSourceLogicalDataType())) {
                    result = false;
                    break;
                }
            } else {
                result = false;
                break;
            }
        }
        return result;
    }

    @Override
    public CustomerSpace getCustomerSpace(String metadataStr) {
        CSVToHdfsConfiguration importConfig;
        try {
            importConfig = JsonUtils.deserialize(metadataStr, CSVToHdfsConfiguration.class);
        } catch (Exception e) {
            throw new RuntimeException("Cannot deserialize CSV import metadata!");
        }
        return importConfig.getCustomerSpace();
    }

    @Override
    public String getConnectorConfig(String metadataStr, String jobIdentifier) {
        CSVToHdfsConfiguration importConfig;
        try {
            importConfig = JsonUtils.deserialize(metadataStr, CSVToHdfsConfiguration.class);
        } catch (Exception e) {
            throw new RuntimeException("Cannot deserialize CSV import metadata!");
        }
        importConfig.setJobIdentifier(jobIdentifier);
        return JsonUtils.serialize(importConfig);
    }
}
