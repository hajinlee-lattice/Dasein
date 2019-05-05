package com.latticeengines.apps.cdl.service.impl;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.avro.Schema.Type;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.collections4.MapUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import com.latticeengines.apps.cdl.service.CDLExternalSystemService;
import com.latticeengines.apps.cdl.service.DataFeedMetadataService;
import com.latticeengines.apps.cdl.util.VdbMetadataUtils;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.cdl.CDLConstants;
import com.latticeengines.domain.exposed.cdl.CDLExternalSystem;
import com.latticeengines.domain.exposed.cdl.CDLImportConfig;
import com.latticeengines.domain.exposed.cdl.CSVImportFileInfo;
import com.latticeengines.domain.exposed.cdl.VdbImportConfig;
import com.latticeengines.domain.exposed.eai.ImportVdbTableConfiguration;
import com.latticeengines.domain.exposed.eai.ImportVdbTableMergeRule;
import com.latticeengines.domain.exposed.eai.SourceType;
import com.latticeengines.domain.exposed.eai.VdbConnectorConfiguration;
import com.latticeengines.domain.exposed.metadata.Attribute;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.pls.VdbLoadTableConfig;
import com.latticeengines.domain.exposed.pls.VdbSpecMetadata;
import com.latticeengines.domain.exposed.query.BusinessEntity;
import com.latticeengines.domain.exposed.serviceapps.core.AttrConfig;
import com.latticeengines.domain.exposed.util.AttributeUtils;

@Component("vdbDataFeedMetadataService")
public class VdbDataFeedMetadataServiceImpl extends DataFeedMetadataService {

    private static final Logger log = LoggerFactory.getLogger(VdbDataFeedMetadataServiceImpl.class);

    public final String DEFAULT_FILE_FORMAT = "%s_%s";

    public final String DATE_FORMAT = "MM-dd-yyyy";

    private static final String[] VDB_ATTR_FIELDS = { "DisplayName", "SourceLogicalDataType", "Description",
            "FundamentalType", "StatisticalType", "DisplayDiscretizationStrategy", "DataQuality", "DataSource",
            "ApprovedUsage", "Tags", "SourceAttrName" };

    public VdbDataFeedMetadataServiceImpl() {
        super(SourceType.VISIDB.getName());
    }

    public Pair<Table, List<AttrConfig>> getMetadata(CDLImportConfig importConfig, String entity) {
        VdbLoadTableConfig vdbLoadTableConfig = ((VdbImportConfig) importConfig).getVdbLoadTableConfig();
        Table metaTable = new Table();
        List<AttrConfig> attrConfigs = new ArrayList<>();
        for (VdbSpecMetadata metadata : vdbLoadTableConfig.getMetadataList()) {
            Attribute attr = VdbMetadataUtils.convertToAttribute(metadata, entity);
            AttrConfig attrConfig = VdbMetadataUtils.getAttrConfig(metadata, entity);
            if (attrConfig != null) {
                attrConfigs.add(attrConfig);
            }
            metaTable.addAttribute(attr);
        }
        metaTable.setPrimaryKey(null);
        metaTable.setName(vdbLoadTableConfig.getTableName());
        metaTable.setDisplayName(vdbLoadTableConfig.getTableName());
        if (VdbMetadataUtils.validateVdbTable(metaTable) && validateOriginalTable(metaTable)) {
            return new ImmutablePair<>(metaTable, attrConfigs);
        } else {
            throw new RuntimeException("The metadata from vdb is not valid!");
        }
    }

    @Override
    public Table resolveMetadata(Table original, Table schemaTable) {
        List<Attribute> attributes = schemaTable.getAttributes();
        HashMap<String, Attribute> originalAttrs = new HashMap<>();
        for (Attribute attr : original.getAttributes()) {
            originalAttrs.put(attr.getName(), attr);
        }
        Set<String> findMatch = new HashSet<>();
        Set<String> originalAttrMatch = new HashSet<>();

        // Match the DL metadata with table in SchemaRepository.
        for (Attribute vdbAttr : original.getAttributes()) {
            String vdbAttrName = vdbAttr.getName();
            for (Attribute interfaceAttr : attributes) {
                String interfaceAttrName = interfaceAttr.getName();
                boolean matched = false;
                if (!findMatch.contains(interfaceAttrName)) {
                    if (interfaceAttrName.equalsIgnoreCase(vdbAttrName)) {
                        matched = true;
                    } else if (interfaceAttr.getAllowedDisplayNames()
                            .contains(vdbAttrName.toUpperCase().replace(" ", "_"))) {
                        matched = true;
                    } else if (interfaceAttr.getAllowedDisplayNames()
                            .contains(vdbAttrName.toUpperCase().replace(" ", ""))) {
                        matched = true;
                    }
                }
                if (matched) {
                    log.info(String.format("Matched column : %s -> %s", vdbAttrName, interfaceAttr));
                    copyAttribute(interfaceAttr, vdbAttr);
                    findMatch.add(interfaceAttrName);
                    originalAttrMatch.add(vdbAttrName);
                    break;
                }
            }
        }

        if (findMatch.size() != attributes.size()) {
            List<String> missingField = new ArrayList<>();
            for (Attribute attr : attributes) {
                if (attr.getRequired() && !findMatch.contains(attr.getName())) {
                    missingField.add(attr.getName());
                }
            }
            if (missingField.size() > 0) {
                throw new RuntimeException(
                        String.format("Missing the following required field: %s", String.join(",", missingField)));
            }
        }

        Iterator<Attribute> attrIterator = attributes.iterator();
        while (attrIterator.hasNext()) {
            Attribute attribute = attrIterator.next();
            if (!findMatch.contains(attribute.getName())) {
                if (attribute.getDefaultValueStr() == null) {
                    log.info(String.format("Remove unmatched column : %s", attribute.getName()));
                    attrIterator.remove();
                } else {
                    log.info(String.format("Keep unmatched column: %s with default value: %s", attribute.getName(),
                            attribute.getDefaultValueStr()));
                }
            }
        }

        originalAttrs.forEach((name, attr) -> {
            if (!originalAttrMatch.contains(name)) {
                attributes.add(attr);
            }
        });

        schemaTable.setName(original.getName());
        schemaTable.setDisplayName(original.getDisplayName());

        if (schemaTable.getLastModifiedKey() != null) {
            String lastModifiedKey = schemaTable.getLastModifiedKey().getName();
            if (schemaTable.getAttribute(lastModifiedKey) == null) {
                log.warn("Cannot map any attribute to designated last modified key " + lastModifiedKey);
                schemaTable.setLastModifiedKey(null);
            }
        }

        schemaTable.deduplicateAttributeNamesIgnoreCase();
        return schemaTable;
    }

    private void copyAttribute(Attribute dest, Attribute source) {
        dest.setSourceAttrName(source.getSourceAttrName());
        dest.setDisplayName(source.getDisplayName());
        dest.setSourceLogicalDataType(source.getSourceLogicalDataType());
        if (StringUtils.isBlank(dest.getPhysicalDataType())) {
            dest.setPhysicalDataType(source.getPhysicalDataType());
        }
        if (StringUtils.isBlank(dest.getSourceLogicalDataType())) {
            dest.setSourceLogicalDataType(source.getPhysicalDataType());
        }
        if (CollectionUtils.isNotEmpty(source.getApprovedUsage())) {
            dest.setApprovedUsage(source.getApprovedUsage());
        }
        dest.setDescription(source.getDescription());
        dest.setDataSource(source.getDataSource());
        if (source.getFundamentalType() != null) {
            dest.setFundamentalType(source.getFundamentalType());
        }
        if (source.getStatisticalType() != null) {
            dest.setStatisticalType(source.getStatisticalType());
        }
        dest.setTags(source.getTags());
        dest.setDisplayDiscretizationStrategy(source.getDisplayDiscretizationStrategy());
        dest.setDataQuality(source.getDataQuality());
        dest.setGroupsViaList(source.getGroupsAsList());
    }

    @Override
    public boolean compareMetadata(Table srcTable, Table targetTable, boolean needSameType) {
        boolean result = false;
        if (srcTable == null || targetTable == null) {
            return result;
        }
        if (needSameType) {
            if (!validateAttribute(srcTable, targetTable)) {
                throw new RuntimeException("Table attribute type should not be changed!");
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
            if (srcAttrs.containsKey(attr.getName())) {
                if (!compareAttribute(srcAttrs.get(attr.getName()), attr)) {
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

    private boolean compareAttribute(Attribute srcAttr, Attribute destAttr) {
        HashSet<String> diffFields = AttributeUtils.diffBetweenAttributes(srcAttr, destAttr);
        if (diffFields == null || diffFields.size() == 0) {
            return true;
        } else {
            for (String field : VDB_ATTR_FIELDS) {
                if (diffFields.contains(field.toLowerCase())) {
                    return false;
                }
            }
            return true;
        }
    }

    private boolean validateAttribute(Table srcTable, Table targetTable) {
        HashMap<String, Attribute> srcAttrs = new HashMap<>();
        for (Attribute attr : srcTable.getAttributes()) {
            srcAttrs.put(attr.getName(), attr);
        }
        for (Attribute attr : targetTable.getAttributes()) {
            if (srcAttrs.containsKey(attr.getName())) {
                Attribute srcAttr = srcAttrs.get(attr.getName());
                if (StringUtils.isEmpty(attr.getSourceLogicalDataType())) {
                    attr.setSourceLogicalDataType(attr.getPhysicalDataType());
                }
                if (StringUtils.isEmpty(srcAttr.getSourceLogicalDataType())) {
                    srcAttr.setSourceLogicalDataType(srcAttr.getPhysicalDataType());
                }
                if (!VdbMetadataUtils.isAcceptableDataType(srcAttr.getSourceLogicalDataType(),
                        attr.getSourceLogicalDataType())) {
                    log.error(String.format("Field %s should have the type %s, not %s", attr.getName(),
                            srcAttrs.get(attr.getName()).getSourceLogicalDataType(), attr.getSourceLogicalDataType()));
                    return false;
                } else {
                    attr.setSourceLogicalDataType(srcAttrs.get(attr.getName()).getSourceLogicalDataType());
                    attr.setPhysicalDataType(srcAttrs.get(attr.getName()).getPhysicalDataType());
                }
            }
        }
        return true;
    }

    @Override
    public CustomerSpace getCustomerSpace(CDLImportConfig importConfig) {
        log.info("Config:" + JsonUtils.serialize(importConfig));
        VdbLoadTableConfig vdbLoadTableConfig = ((VdbImportConfig) importConfig).getVdbLoadTableConfig();
        return CustomerSpace.parse(vdbLoadTableConfig.getTenantId());
    }

    @Override
    public String getConnectorConfig(CDLImportConfig importConfig, String jobIdentifier) {
        VdbLoadTableConfig vdbLoadTableConfig = ((VdbImportConfig) importConfig).getVdbLoadTableConfig();
        VdbConnectorConfiguration vdbConnectorConfiguration = new VdbConnectorConfiguration();
        vdbConnectorConfiguration.setGetQueryDataEndpoint(vdbLoadTableConfig.getGetQueryDataEndpoint());
        vdbConnectorConfiguration.setReportStatusEndpoint(vdbLoadTableConfig.getReportStatusEndpoint());
        vdbConnectorConfiguration.setDlDataReady(true);
        ImportVdbTableConfiguration importVdbTableConfiguration = new ImportVdbTableConfiguration();
        importVdbTableConfiguration.setBatchSize(vdbLoadTableConfig.getBatchSize());
        importVdbTableConfiguration.setDataCategory(vdbLoadTableConfig.getDataCategory());
        importVdbTableConfiguration.setCollectionIdentifier(jobIdentifier);
        importVdbTableConfiguration.setVdbQueryHandle(vdbLoadTableConfig.getVdbQueryHandle());
        importVdbTableConfiguration.setMergeRule(ImportVdbTableMergeRule.getByName(vdbLoadTableConfig.getMergeRule()));
        importVdbTableConfiguration.setCreateTableRule(vdbLoadTableConfig.getCreateTableRule());
        importVdbTableConfiguration.setMetadataList(vdbLoadTableConfig.getMetadataList());
        importVdbTableConfiguration.setTotalRows(vdbLoadTableConfig.getTotalRows());

        vdbConnectorConfiguration.addTableConfiguration(vdbLoadTableConfig.getTableName(), importVdbTableConfiguration);
        String vdbConnectorConfigurationStr = JsonUtils.serialize(vdbConnectorConfiguration);

        return vdbConnectorConfigurationStr;
    }

    @Override
    public Type getAvroType(Attribute attribute) {
        Type type = null;
        if (attribute.getPhysicalDataType() == null) {
            throw new RuntimeException(
                    String.format("Physical data type for attribute %s is null", attribute.getName()));
        }
        String typeStrLowerCase = attribute.getPhysicalDataType().toLowerCase();
        switch (typeStrLowerCase) {
            case "bit":
            case "boolean":
                type = Type.BOOLEAN;
                break;
            case "byte":
            case "short":
            case "int":
                type = Type.INT;
                break;
            case "long":
            case "date":
            case "datetime":
            case "datetimeoffset":
                type = Type.LONG;
                break;
            case "float":
                type = Type.FLOAT;
                break;
            case "double":
                type = Type.DOUBLE;
                break;
            case "string":
                type = Type.STRING;
                break;
            default:
                break;
        }
        if (type == null) {
            if (typeStrLowerCase.startsWith("nvarchar") || typeStrLowerCase.startsWith("varchar")) {
                type = Type.STRING;
            }
        }
        return type;
    }

    @Override
    public void autoSetCDLExternalSystem(CDLExternalSystemService cdlExternalSystemService, Table table,
            String customerSpace) {
        if (cdlExternalSystemService == null || table == null) {
            return;
        }
        Map<String, String> crmAttrMap = table.getAttributes().stream()
                .filter(attr -> attr.getName().equalsIgnoreCase("CRMAccount_External_ID")
                        || attr.getName().equalsIgnoreCase("SalesforceAccountId")
                        || attr.getName().equalsIgnoreCase(USER_PREFIX + "CRMAccount_External_ID")
                        || attr.getName().equalsIgnoreCase(USER_PREFIX + "SalesforceAccountId"))
                .collect(Collectors.toMap(Attribute::getName, Attribute::getDisplayName));

        if (MapUtils.isNotEmpty(crmAttrMap)) {
            List<Pair<String, String>> idMappings = new ArrayList<>();
            CDLExternalSystem cdlExternalSystem = new CDLExternalSystem();
            cdlExternalSystem.setCRMIdList(crmAttrMap.keySet().stream().collect(Collectors.toList()));
            cdlExternalSystem.setEntity(BusinessEntity.Account);
            crmAttrMap.forEach((attrName, displayName) ->
                    idMappings.add(Pair.of(attrName, StringUtils.isBlank(displayName) ? attrName : displayName)));
            cdlExternalSystem.setIdMapping(idMappings);
            cdlExternalSystemService.createOrUpdateExternalSystem(customerSpace, cdlExternalSystem, BusinessEntity.Account);
        }
    }

    @Override
    public boolean validateOriginalTable(Table original) {
        if (!super.validateOriginalTable(original)) {
            log.error("The original table cannot be null!");
            return false;
        }
        // 1. All data types from source should be recognizable.
        for (Attribute attr : original.getAttributes()) {
            Type avroType = getAvroType(attr);
            if (avroType == null) {
                log.error(String.format("Not supported type %s for column %s", attr.getPhysicalDataType(), attr.getSourceAttrName()));
                return false;
            }
        }
        return true;
    }

    @Override
    // Implement this method for VisiDB import purely for UI purpose
    public CSVImportFileInfo getImportFileInfo(CDLImportConfig importConfig) {
        CSVImportFileInfo csvImportFileInfo = new CSVImportFileInfo();
        csvImportFileInfo.setReportFileDisplayName(String.format(DEFAULT_FILE_FORMAT, SourceType.VISIDB.getName(),
                new SimpleDateFormat(DATE_FORMAT).format(new Date())));
        csvImportFileInfo.setReportFileName(String.format(DEFAULT_FILE_FORMAT, SourceType.VISIDB.getName(),
                new SimpleDateFormat(DATE_FORMAT).format(new Date())));
        csvImportFileInfo.setFileUploadInitiator(CDLConstants.DEFAULT_VISIDB_USER);
        return csvImportFileInfo;
    }
}
