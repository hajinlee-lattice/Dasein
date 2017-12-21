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

import com.latticeengines.apps.cdl.service.CDLExternalSystemService;
import com.latticeengines.apps.cdl.service.DataFeedMetadataService;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.cdl.CDLImportConfig;
import com.latticeengines.domain.exposed.cdl.CSVImportConfig;
import com.latticeengines.domain.exposed.cdl.CSVImportFileInfo;
import com.latticeengines.domain.exposed.eai.CSVToHdfsConfiguration;
import com.latticeengines.domain.exposed.eai.SourceType;
import com.latticeengines.domain.exposed.metadata.Attribute;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.propdata.manage.ColumnSelection;
import com.latticeengines.domain.exposed.query.BusinessEntity;
import com.latticeengines.proxy.exposed.metadata.MetadataProxy;

@Component("csvDataFeedMetadataService")
public class CSVDataFeedMetadataServiceImpl extends DataFeedMetadataService {

    private static final Logger log = LoggerFactory.getLogger(CSVDataFeedMetadataServiceImpl.class);

    @Autowired
    private MetadataProxy metadataProxy;

    protected CSVDataFeedMetadataServiceImpl() {
        super(SourceType.FILE.getName());
    }

    public Table getMetadata(CDLImportConfig importConfig, String entity) {
        CSVImportConfig csvImportConfig = (CSVImportConfig) importConfig;
        log.info("Template table name: " + csvImportConfig.getCsvToHdfsConfiguration().getTemplateName());
        Table metaTable = metadataProxy.getTable(
                csvImportConfig.getCsvToHdfsConfiguration().getCustomerSpace().toString(),
                csvImportConfig.getCsvToHdfsConfiguration().getTemplateName());
        if (BusinessEntity.getByName(entity) == BusinessEntity.Account) {
            List<ColumnSelection.Predefined> groups = new ArrayList<>();
            groups.add(ColumnSelection.Predefined.TalkingPoint);
            for (Attribute attribute : metaTable.getAttributes()) {
                attribute.setGroupsViaList(groups);
            }
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
                            srcAttrs.get(attr.getName()).getPhysicalDataType(), attr.getPhysicalDataType()));
                    return false;
                }
            }
        }
        return true;
    }

    @Override
    public CustomerSpace getCustomerSpace(CDLImportConfig importConfig) {
        CSVToHdfsConfiguration csvmportConfig = ((CSVImportConfig) importConfig).getCsvToHdfsConfiguration();
        return csvmportConfig.getCustomerSpace();
    }

    @Override
    public String getConnectorConfig(CDLImportConfig importConfig, String jobIdentifier) {
        CSVToHdfsConfiguration csvmportConfig = ((CSVImportConfig) importConfig).getCsvToHdfsConfiguration();
        ;
        csvmportConfig.setJobIdentifier(jobIdentifier);
        return JsonUtils.serialize(csvmportConfig);
    }

    @Override
    public Type getAvroType(Attribute attribute) {
        if (attribute == null) {
            return null;
        } else {
            return Type.valueOf(attribute.getPhysicalDataType().toUpperCase());
        }
    }

    @Override
    public void autoSetCDLExternalSystem(CDLExternalSystemService cdlExternalSystemService, Table table,
            String customerSpace) {
        return;
    }

    @Override
    public CSVImportFileInfo getImportFileInfo(CDLImportConfig importConfig) {
        CSVImportConfig csvImportConfig = (CSVImportConfig) importConfig;
        return csvImportConfig.getCSVImportFileInfo();
    }
}
