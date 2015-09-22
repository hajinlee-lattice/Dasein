package com.latticeengines.pls.service.impl;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.exception.LedpCode;
import com.latticeengines.domain.exposed.exception.LedpException;
import com.latticeengines.domain.exposed.pls.VdbMetadataField;
import com.latticeengines.domain.exposed.security.Tenant;
import com.latticeengines.liaison.exposed.service.ConnectionMgr;
import com.latticeengines.liaison.exposed.service.ConnectionMgrFactory;
import com.latticeengines.liaison.exposed.service.Query;
import com.latticeengines.liaison.exposed.service.QueryColumn;
import com.latticeengines.pls.service.VdbMetadataConstants;
import com.latticeengines.pls.service.TenantConfigService;
import com.latticeengines.pls.service.VdbMetadataService;

@Component("vdbMetadataService")
public class VdbMetadataServiceImpl implements VdbMetadataService {

    @Autowired
    private TenantConfigService tenantConfigService;

    @Autowired
    private ConnectionMgrFactory connectionMgrFactory;

    @Override
    public List<VdbMetadataField> getFields(Tenant tenant) {
        try {
            String tenantName = CustomerSpace.parse(tenant.getId()).getTenantId();
            String dlUrl = tenantConfigService.getDLRestServiceAddress(tenant.getId());
            ConnectionMgr connMgr = connectionMgrFactory.getConnectionMgr("visiDB", tenantName, dlUrl);
            Map<String, Map<String, String>> colsMetadata = connMgr.getMetadata(VdbMetadataConstants.MODELING_QUERY_NAME);
            List<VdbMetadataField> fields = getFields(colsMetadata);

            return fields;
        } catch (Exception ex) {
            throw new LedpException(LedpCode.LEDP_18046, ex, new String[] { ex.getMessage() });
        }
    }

    private List<VdbMetadataField> getFields(Map<String, Map<String, String>> columnsMetadata) {
        List<VdbMetadataField> fields = new ArrayList<VdbMetadataField>();
        for (Map.Entry<String, Map<String, String>> entry : columnsMetadata.entrySet()) {
            VdbMetadataField field = new VdbMetadataField();
            field.setColumnName(entry.getKey());
            Map<String, String> attributes = entry.getValue();
            field.setSource(getFieldValue(attributes, VdbMetadataConstants.ATTRIBUTE_SOURCE));
            field.setCategory(getFieldValue(attributes, VdbMetadataConstants.ATTRIBUTE_CATEGORY));
            field.setDisplayName(getFieldValue(attributes, VdbMetadataConstants.ATTRIBUTE_DISPLAYNAME));
            field.setDescription(getFieldValue(attributes, VdbMetadataConstants.ATTRIBUTE_DESCRIPTION));
            field.setApprovedUsage(getFieldValue(attributes, VdbMetadataConstants.ATTRIBUTE_APPROVED_USAGE));
            field.setTags(getFieldValue(attributes, VdbMetadataConstants.ATTRIBUTE_TAGS));
            field.setFundamentalType(getFieldValue(attributes, VdbMetadataConstants.ATTRIBUTE_FUNDAMENTAL_TYPE));
            field.setDisplayDiscretization(getFieldValue(attributes, VdbMetadataConstants.ATTRIBUTE_DISPLAY_DISCRETIZATION));
            field.setStatisticalType(getFieldValue(attributes, VdbMetadataConstants.ATTRIBUTE_STATISTICAL_TYPE));
            String sourceToDisplay = getSourceToDisplay(field.getSource());
            field.setSourceToDisplay(sourceToDisplay);
            fields.add(field);
        }

        return fields;
    }

    private String getFieldValue(Map<String, String> map, String key) {
        if (map.containsKey(key)) {
            String value = map.get(key);
            if (VdbMetadataConstants.ATTRIBUTE_FUNDAMENTAL_TYPE.equals(key) &&
                    VdbMetadataConstants.ATTRIBUTE_FUNDAMENTAL_UNKNOWN_VALUE.equalsIgnoreCase(value)) {
                return null;
            } else if (VdbMetadataConstants.ATTRIBUTE_NULL_VALUE.equalsIgnoreCase(value)) {
                return null;
            } else {
                return value;
            }
        } else {
            return null;
        }
    }

    @Override
    public String getSourceToDisplay(String source) {
        if (source == null) {
            return "";
        }

        boolean exist = VdbMetadataConstants.SOURCE_MAPPING.containsKey(source);
        if (exist) {
            return VdbMetadataConstants.SOURCE_MAPPING.get(source);
        } else {
            return source;
        }
    }

    @Override
    public void UpdateField(Tenant tenant, VdbMetadataField field) {
        try {
            String tenantName = CustomerSpace.parse(tenant.getId()).getTenantId();
            String dlUrl = tenantConfigService.getDLRestServiceAddress(tenant.getId());
            ConnectionMgr connMgr = connectionMgrFactory.getConnectionMgr("visiDB", tenantName, dlUrl);

            Query modelingQuery = connMgr.getQuery(VdbMetadataConstants.MODELING_QUERY_NAME);
            if (modelingQuery == null) {
                throw new LedpException(LedpCode.LEDP_18050, new String[] { VdbMetadataConstants.MODELING_QUERY_NAME });
            }
            Query customQuery = connMgr.getQuery(VdbMetadataConstants.CUSTOM_QUERY_NAME);
            if (customQuery == null) {
                throw new LedpException(LedpCode.LEDP_18050, new String[] { VdbMetadataConstants.CUSTOM_QUERY_NAME });
            }
            QueryColumn queryColumn = modelingQuery.getColumn(field.getColumnName());
            if (queryColumn == null) {
                throw new LedpException(LedpCode.LEDP_18051, new String[] { field.getColumnName() });
            }

            Map<String, String> metadata = getMetadataMap(field);
            queryColumn.setMetadata(metadata);
            modelingQuery.updateColumn(queryColumn);
            customQuery.updateColumn(queryColumn);
            connMgr.setQuery(modelingQuery);
            connMgr.setQuery(customQuery);
        } catch (Exception ex) {
            throw new LedpException(LedpCode.LEDP_18047, ex, new String[] { ex.getMessage() });
        }
    }

    @Override
    public void UpdateFields(Tenant tenant, List<VdbMetadataField> fields) {
        try {
            String tenantName = CustomerSpace.parse(tenant.getId()).getTenantId();
            String dlUrl = tenantConfigService.getDLRestServiceAddress(tenant.getId());
            ConnectionMgr connMgr = connectionMgrFactory.getConnectionMgr("visiDB", tenantName, dlUrl);

            Query modelingQuery = connMgr.getQuery(VdbMetadataConstants.MODELING_QUERY_NAME);
            if (modelingQuery == null) {
                throw new LedpException(LedpCode.LEDP_18050, new String[] { VdbMetadataConstants.MODELING_QUERY_NAME });
            }
            Query customQuery = connMgr.getQuery(VdbMetadataConstants.CUSTOM_QUERY_NAME);
            if (customQuery == null) {
                throw new LedpException(LedpCode.LEDP_18050, new String[] { VdbMetadataConstants.CUSTOM_QUERY_NAME });
            }

            for (Integer i = 0; i < fields.size(); i++) {
                VdbMetadataField field = fields.get(i);
                QueryColumn queryColumn = modelingQuery.getColumn(field.getColumnName());
                if (queryColumn == null) {
                    throw new LedpException(LedpCode.LEDP_18051, new String[] { field.getColumnName() });
                }

                Map<String, String> metadata = getMetadataMap(field);
                queryColumn.setMetadata(metadata);
                modelingQuery.updateColumn(queryColumn);
                customQuery.updateColumn(queryColumn);
            }
            connMgr.setQuery(modelingQuery);
            connMgr.setQuery(customQuery);
        } catch (Exception ex) {
            throw new LedpException(LedpCode.LEDP_18048, ex, new String[] { ex.getMessage() });
        }
    }

    private Map<String, String> getMetadataMap(VdbMetadataField field) {
        Map<String, String> metadata = new HashMap<>();
        String displayName = field.getDisplayName() == null ? "" : field.getDisplayName();
        metadata.put(VdbMetadataConstants.ATTRIBUTE_DISPLAYNAME, displayName);
        String tags = field.getTags();
        if (tags != null && !tags.isEmpty()) {
            metadata.put(VdbMetadataConstants.ATTRIBUTE_TAGS, tags);
        }
        String category = field.getCategory();
        if (category != null && !category.isEmpty()) {
            metadata.put(VdbMetadataConstants.ATTRIBUTE_CATEGORY, category);
        }
        String approvedUsage = field.getApprovedUsage();
        if (approvedUsage != null && !approvedUsage.isEmpty()) {
            metadata.put(VdbMetadataConstants.ATTRIBUTE_APPROVED_USAGE, approvedUsage);
        }
        String fundamentalType = field.getFundamentalType();
        if (fundamentalType != null && !fundamentalType.isEmpty() &&
                !VdbMetadataConstants.ATTRIBUTE_FUNDAMENTAL_UNKNOWN_VALUE.equalsIgnoreCase(fundamentalType)) {
            metadata.put(VdbMetadataConstants.ATTRIBUTE_FUNDAMENTAL_TYPE, fundamentalType);
        }
        String description = field.getDescription() == null ? "" : field.getDescription();
        metadata.put(VdbMetadataConstants.ATTRIBUTE_DESCRIPTION, description);
        String displayDiscretization = field.getDisplayDiscretization() == null ? "" : field.getDisplayDiscretization();
        metadata.put(VdbMetadataConstants.ATTRIBUTE_DISPLAY_DISCRETIZATION, displayDiscretization);
        String statisticalType = field.getStatisticalType();
        if (statisticalType != null && !statisticalType.isEmpty()) {
            metadata.put(VdbMetadataConstants.ATTRIBUTE_STATISTICAL_TYPE, statisticalType);
        }

        return metadata;
    }

}