package com.latticeengines.apps.cdl.service.impl;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import com.latticeengines.apps.cdl.service.DataFeedMetadataService;
import com.latticeengines.apps.cdl.util.VdbMetadataUtils;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.eai.ImportVdbTableConfiguration;
import com.latticeengines.domain.exposed.eai.SourceType;
import com.latticeengines.domain.exposed.eai.VdbConnectorConfiguration;
import com.latticeengines.domain.exposed.metadata.Attribute;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.pls.VdbLoadTableConfig;
import com.latticeengines.domain.exposed.pls.VdbSpecMetadata;
import com.latticeengines.domain.exposed.util.AttributeUtils;

import edu.emory.mathcs.backport.java.util.Collections;

@Component("vdbDataFeedMetadataService")
public class VdbDataFeedMetadataServiceImpl extends DataFeedMetadataService {

    private static final Logger log = LoggerFactory.getLogger(VdbDataFeedMetadataServiceImpl.class);

    private static final String[] VDB_ATTR_FIELDS = {"DisplayName", "SourceLogicalDataType", "Description",
            "FundamentalType", "StatisticalType", "DisplayDiscretizationStrategy", "DataQuality", "DataSource",
            "ApprovedUsage", "Tags"};

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
            Attribute attr = VdbMetadataUtils.convertToAttribute(metadata);
            metaTable.addAttribute(attr);
        }
        metaTable.setPrimaryKey(null);
        metaTable.setName(vdbLoadTableConfig.getTableName());
        metaTable.setDisplayName(vdbLoadTableConfig.getTableName());
        return metaTable;
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
        for (Attribute vdbAttr: original.getAttributes()) {
            String vdbAttrName = vdbAttr.getName();
            for (Attribute interfaceAttr: attributes) {
                String interfaceAttrName = interfaceAttr.getName();
                boolean matched = false;
                if (!findMatch.contains(interfaceAttrName)) {
                    if (interfaceAttrName.equalsIgnoreCase(vdbAttrName)) {
                        matched = true;
                    } else if (interfaceAttr.getAllowedDisplayNames().contains(vdbAttrName.toUpperCase())) {
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
                if (!attr.isNullable() && !findMatch.contains(attr.getName())) {
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
                log.info(String.format("Remove unmatched column : %s", attribute.getName()));
                attrIterator.remove();
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

        return schemaTable;
    }

    private void copyAttribute(Attribute dest, Attribute source) {
        dest.setSourceAttrName(source.getSourceAttrName());
        dest.setDisplayName(source.getDisplayName());
        dest.setSourceLogicalDataType(source.getSourceLogicalDataType());
        dest.setPhysicalDataType(source.getPhysicalDataType());
        if (StringUtils.isBlank(dest.getSourceLogicalDataType())) {
            dest.setSourceLogicalDataType(source.getPhysicalDataType());
        }
        dest.setApprovedUsage(source.getApprovedUsage());
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
                if (!VdbMetadataUtils.isAcceptableDataType(srcAttrs.get(attr.getName()).getSourceLogicalDataType
                        (), attr.getSourceLogicalDataType())) {
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
