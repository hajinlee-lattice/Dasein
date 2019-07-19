package com.latticeengines.apps.cdl.service.impl;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.avro.Schema.Type;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
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
import com.latticeengines.domain.exposed.serviceapps.core.AttrConfig;
import com.latticeengines.proxy.exposed.metadata.MetadataProxy;

@Component("csvDataFeedMetadataService")
public class CSVDataFeedMetadataServiceImpl extends DataFeedMetadataService {

    private static final Logger log = LoggerFactory.getLogger(CSVDataFeedMetadataServiceImpl.class);

    @Autowired
    private MetadataProxy metadataProxy;

    protected CSVDataFeedMetadataServiceImpl() {
        super(SourceType.FILE.getName());
    }

    public Pair<Table, List<AttrConfig>> getMetadata(CDLImportConfig importConfig, String entity) {
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
        if (validateOriginalTable(metaTable)) {
            return new ImmutablePair<>(metaTable, new ArrayList<>());
        } else {
            throw new RuntimeException("The metadata from csv import is not valid!");
        }
    }

    @Override
    public Table resolveMetadata(Table original, Table schemaTable) {
        if (schemaTable == null) {
            return original;
        }
        List<Attribute> attributes = schemaTable.getAttributes();
        Map<String, Attribute> requiredAttr = new HashMap<>();
        for (Attribute attribute : attributes) {
            if (Boolean.TRUE.equals(attribute.getRequired()) && attribute.getDefaultValueStr() == null) {
                requiredAttr.put(attribute.getName(), attribute);
            }
        }
        for (Attribute attribute : original.getAttributes()) {
            requiredAttr.remove(attribute.getName());
        }
        if (requiredAttr.size() > 0) {
            throw new RuntimeException(String.format("Missing the following required field: %s",
                    String.join(",", requiredAttr.keySet())));
        }
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
            } else {
                if (!attr.getDisplayName().equals(srcAttrs.get(attr.getName()).getDisplayName())) {
                    result = false;
                    break;
                }
                if (!StringUtils.equals(attr.getDateFormatString(),
                        srcAttrs.get(attr.getName()).getDateFormatString())) {
                    result = false;
                    break;
                }
                if (!StringUtils.equals(attr.getTimeFormatString(),
                        srcAttrs.get(attr.getName()).getTimeFormatString())) {
                    result = false;
                    break;
                }
                if (!StringUtils.equals(attr.getTimezone(),
                        srcAttrs.get(attr.getName()).getTimezone())) {
                    result = false;
                    break;
                }
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
    }

    @Override
    public boolean validateOriginalTable(Table original) {
        if (!super.validateOriginalTable(original)) {
            log.error("The original table cannot be null!");
            return false;
        }
        Set<String> attrNames = new HashSet<>();
        for (Attribute attribute : original.getAttributes()) {
            if (attrNames.contains(attribute.getName().toLowerCase())) {
                log.error(String.format("Table already have attribute with same name %s (case insensitive)",
                        attribute.getName()));
                return false;
            } else {
                attrNames.add(attribute.getName().toLowerCase());
            }
        }
        return true;
    }

    @Override
    public CSVImportFileInfo getImportFileInfo(CDLImportConfig importConfig) {
        CSVImportConfig csvImportConfig = (CSVImportConfig) importConfig;
        return csvImportConfig.getCSVImportFileInfo();
    }
}
