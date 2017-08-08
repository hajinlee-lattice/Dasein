package com.latticeengines.apps.cdl.service.impl;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import com.latticeengines.apps.cdl.service.DataFeedMetadataService;
import com.latticeengines.common.exposed.util.AvroUtils;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.eai.ImportConfiguration;
import com.latticeengines.domain.exposed.eai.ImportVdbTableConfiguration;
import com.latticeengines.domain.exposed.eai.SourceType;
import com.latticeengines.domain.exposed.eai.VdbConnectorConfiguration;
import com.latticeengines.domain.exposed.metadata.Attribute;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.metadata.standardschemas.SchemaRepository;
import com.latticeengines.domain.exposed.pls.SchemaInterpretation;
import com.latticeengines.domain.exposed.pls.VdbLoadTableConfig;
import com.latticeengines.domain.exposed.pls.VdbSpecMetadata;

@Component("vdbDataFeedMetadataService")
public class VdbDataFeedMetadataServiceImpl extends DataFeedMetadataService {

    private static final Logger log = LoggerFactory.getLogger(VdbDataFeedMetadataServiceImpl.class);

    public VdbDataFeedMetadataServiceImpl() {
        super(SourceType.VISIDB.getName());
    }

    @Override
    public Table getMetadata(String metadataStr) {
        VdbLoadTableConfig vdbLoadTableConfig = null;
        try {
            vdbLoadTableConfig = JsonUtils.deserialize(metadataStr, VdbLoadTableConfig.class);
        } catch (Exception e) {
            throw new RuntimeException("Cannot deserialize vdb table metadata!");
        }
        Table metaTable = new Table();
        for (VdbSpecMetadata metadata : vdbLoadTableConfig.getMetadataList()) {
            Attribute attr = new Attribute();
            attr.setName(AvroUtils.getAvroFriendlyString(metadata.getColumnName()));
            attr.setSourceAttrName(metadata.getColumnName());
            attr.setDisplayName(metadata.getDisplayName());
            attr.setSourceLogicalDataType(metadata.getDataType());
            attr.setPhysicalDataType(metadata.getDataType());
            attr.setApprovedUsage(metadata.getApprovedUsage());
            attr.setDescription(metadata.getDescription());
            attr.setDataSource(metadata.getDataSource());
            attr.setFundamentalType(metadata.getFundamentalType());
            attr.setStatisticalType(metadata.getStatisticalType());
            attr.setTags(metadata.getTags());
            attr.setDisplayDiscretizationStrategy(metadata.getDisplayDiscretizationStrategy());
            if (metadata.getDataQuality() != null && metadata.getDataQuality().size() > 0) {
                attr.setDataQuality(metadata.getDataQuality().get(0));
            }
            metaTable.addAttribute(attr);
        }
        metaTable.setPrimaryKey(null);
        metaTable.setName(vdbLoadTableConfig.getTableName());
        metaTable.setDisplayName(vdbLoadTableConfig.getTableName());
        return metaTable;
    }

    @Override
    public Table resolveMetadata(Table original, SchemaInterpretation schemaInterpretation) {
        Table table = SchemaRepository.instance().getSchema(schemaInterpretation);
        List<Attribute> attributes = table.getAttributes();
        HashMap<String, Attribute> originalAttrs = new HashMap<>();
        for (Attribute attr : original.getAttributes()) {
            originalAttrs.put(attr.getName(), attr);
        }
        Set<String> findMatch = new HashSet<>();
        Set<String> originalAttrMatch = new HashSet<>();
        // Match the DL metadata with table in SchemaRepository.
        for (Map.Entry<String, Attribute> entry : originalAttrs.entrySet()) {
            Iterator<Attribute> attrIterator = attributes.iterator();
            while (attrIterator.hasNext()) {
                Attribute attribute = attrIterator.next();
                if (entry.getKey().equalsIgnoreCase(attribute.getName())) {
                    attribute.setDisplayName(entry.getValue().getName());
                    attribute.setSourceLogicalDataType(entry.getValue().getSourceLogicalDataType());
                    attribute.setPhysicalDataType(entry.getValue().getPhysicalDataType());
                    originalAttrMatch.add(entry.getKey());
                    findMatch.add(attribute.getName());
                    break;
                }
                if (attribute.getAllowedDisplayNames().contains(entry.getKey().toUpperCase())) {
                    log.info(String.format("Matched column : %s", entry.getKey()));
                    attribute.setDisplayName(entry.getValue().getName());
                    attribute.setSourceLogicalDataType(entry.getValue().getSourceLogicalDataType());
                    attribute.setPhysicalDataType(entry.getValue().getPhysicalDataType());
                    originalAttrMatch.add(entry.getKey());
                    findMatch.add(attribute.getName());
                    break;
                }
                // Remove nullable (not required) field in SchemaRepository.
                if (attribute.isNullable()) {
                    attrIterator.remove();
                }
            }
        }

        if (findMatch.size() != attributes.size()) {
            List<String> missingField = new ArrayList<>();
            for (Attribute attr : attributes) {
                if (!attr.isNullable() && !findMatch.contains(attr.getName())) {
                    missingField.add(attr.getName());
                }
            }
            if (missingField.size() > 0) {
                throw new RuntimeException(
                        String.format("Missing the following required field: %s", String.join(",", missingField)));
            }
        }

        for (Map.Entry<String, Attribute> entry : originalAttrs.entrySet()) {
            if (!originalAttrMatch.contains(entry.getKey())) {
                attributes.add(entry.getValue());
            }
        }

        table.setName(original.getName());
        table.setDisplayName(original.getDisplayName());
        return table;
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

    private boolean validateAttribute(Table srcTable, Table targetTable) {
        HashMap<String, Attribute> srcAttrs = new HashMap<>();
        for (Attribute attr : srcTable.getAttributes()) {
            srcAttrs.put(attr.getName(), attr);
        }
        for (Attribute attr : targetTable.getAttributes()) {
            if (srcAttrs.containsKey(attr.getName())) {
                if (!StringUtils.equals(srcAttrs.get(attr.getName()).getSourceLogicalDataType(),
                        attr.getSourceLogicalDataType())) {
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
        VdbLoadTableConfig vdbLoadTableConfig;
        try {
            vdbLoadTableConfig = JsonUtils.deserialize(metadataStr, VdbLoadTableConfig.class);
        } catch (Exception e) {
            throw new RuntimeException("Cannot deserialize vdb table metadata!");
        }

        return CustomerSpace.parse(vdbLoadTableConfig.getTenantId());
    }

    @Override
    public String getConnectorConfig(String metadataStr, String jobIdentifier) {
        VdbLoadTableConfig vdbLoadTableConfig = null;
        try {
            vdbLoadTableConfig = JsonUtils.deserialize(metadataStr, VdbLoadTableConfig.class);
        } catch (Exception e) {
            throw new RuntimeException("Cannot deserialize vdb table metadata!");
        }
        VdbConnectorConfiguration vdbConnectorConfiguration = new VdbConnectorConfiguration();
        vdbConnectorConfiguration.setGetQueryDataEndpoint(vdbLoadTableConfig.getGetQueryDataEndpoint());
        vdbConnectorConfiguration.setReportStatusEndpoint(vdbLoadTableConfig.getReportStatusEndpoint());
        vdbConnectorConfiguration.setDlDataReady(true);
        ImportVdbTableConfiguration importVdbTableConfiguration = new ImportVdbTableConfiguration();
        importVdbTableConfiguration.setBatchSize(vdbLoadTableConfig.getBatchSize());
        importVdbTableConfiguration.setDataCategory(vdbLoadTableConfig.getDataCategory());
        importVdbTableConfiguration.setCollectionIdentifier(jobIdentifier);
        importVdbTableConfiguration.setVdbQueryHandle(vdbLoadTableConfig.getVdbQueryHandle());
        importVdbTableConfiguration.setMergeRule(vdbLoadTableConfig.getMergeRule());
        importVdbTableConfiguration.setCreateTableRule(vdbLoadTableConfig.getCreateTableRule());
        importVdbTableConfiguration.setMetadataList(vdbLoadTableConfig.getMetadataList());
        importVdbTableConfiguration.setTotalRows(vdbLoadTableConfig.getTotalRows());

        vdbConnectorConfiguration.addTableConfiguration(vdbLoadTableConfig.getTableName(), importVdbTableConfiguration);
        String vdbConnectorConfigurationStr = JsonUtils.serialize(vdbConnectorConfiguration);

        return vdbConnectorConfigurationStr;
    }

    @Override
    public boolean needUpdateDataFeedStatus() {
        return true;
    }
}
