package com.latticeengines.pls.service.impl;

import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.dataloader.SourceTableMetadataResult;
import com.latticeengines.domain.exposed.dataloader.SourceTableMetadataResult.SourceColumnMetadata;
import com.latticeengines.domain.exposed.exception.LedpCode;
import com.latticeengines.domain.exposed.exception.LedpException;
import com.latticeengines.domain.exposed.pls.LeadEnrichmentAttribute;
import com.latticeengines.domain.exposed.metadata.ColumnMetadata;
import com.latticeengines.domain.exposed.propdata.manage.ColumnSelection;
import com.latticeengines.domain.exposed.security.Tenant;
import com.latticeengines.liaison.exposed.service.ConnectionMgr;
import com.latticeengines.liaison.exposed.service.ConnectionMgrFactory;
import com.latticeengines.liaison.exposed.service.LPFunctions;
import com.latticeengines.pls.service.LeadEnrichmentService;
import com.latticeengines.pls.service.TenantConfigService;
import com.latticeengines.proxy.exposed.propdata.ColumnMetadataProxy;
import com.latticeengines.remote.exposed.service.DataLoaderService;

@Component("leadEnrichmentService")
public class LeadEnrichmentServiceImpl implements LeadEnrichmentService {

    private static final String CONNECTION_MGR_TYPE = "visiDB";

    @Autowired
    private TenantConfigService tenantConfigService;

    @Autowired
    private ConnectionMgrFactory connectionMgrFactory;

    @Autowired
    private LPFunctions lpFunctions;

    @Autowired
    private ColumnMetadataProxy columnMetadataProxy;

    @Autowired
    private DataLoaderService dataLoaderService;

    @Override
    public List<LeadEnrichmentAttribute> getAvailableAttributes() {
        try {
            List<LeadEnrichmentAttribute> attributes = new ArrayList<LeadEnrichmentAttribute>();
            List<ColumnMetadata> columns = columnMetadataProxy
                    .columnSelection(ColumnSelection.Predefined.LeadEnrichment);
            for (ColumnMetadata column : columns) {
                LeadEnrichmentAttribute attribute = toLeadEnrichmentAttribute(column);
                attributes.add(attribute);
            }
            return attributes;
        } catch (Exception ex) {
            throw new LedpException(LedpCode.LEDP_18077, ex, new String[] { ex.getMessage() });
        }
    }

    private LeadEnrichmentAttribute toLeadEnrichmentAttribute(ColumnMetadata columnMetadata) {
        LeadEnrichmentAttribute attribute = new LeadEnrichmentAttribute();
        String columnName = columnMetadata.getColumnName();
        attribute.setFieldName(columnName);
        attribute.setFieldNameInTarget(lpFunctions.fieldNameRestrictDefaultLength(columnName));
        attribute.setFieldType(columnMetadata.getDataType());
        attribute.setDisplayName(columnMetadata.getDisplayName());
        attribute.setDataSource(columnMetadata.getMatchDestination());
        attribute.setDescription(columnMetadata.getDescription());
        return attribute;
    }

    @Override
    public List<LeadEnrichmentAttribute> getAttributes(Tenant tenant) {
        try {
            List<LeadEnrichmentAttribute> attributes = new ArrayList<LeadEnrichmentAttribute>();

            Map<String, Boolean> sourceMap = new HashMap<String, Boolean>();
            List<LeadEnrichmentAttribute> attrs = getAvailableAttributes();
            for (LeadEnrichmentAttribute attr : attrs) {
                String source = attr.getDataSource();
                if (source != null && source.length() > 0) {
                    sourceMap.put(source, true);
                }
            }
            if (!sourceMap.isEmpty()) {
                String tenantName = CustomerSpace.parse(tenant.getId()).getTenantId();
                String dlUrl = tenantConfigService.getDLRestServiceAddress(tenant.getId());
                ConnectionMgr connMgr = connectionMgrFactory.getConnectionMgr(CONNECTION_MGR_TYPE,
                        tenantName, dlUrl);
                AbstractMap.SimpleImmutableEntry<String, String> typeAndVersions = lpFunctions
                        .getLPTemplateTypeAndVersion(connMgr);
                for (String source : sourceMap.keySet()) {
                    Map<String, String> map = lpFunctions.getLDCWritebackAttributes(connMgr,
                            source, typeAndVersions.getValue());
                    for (Entry<String, String> entry : map.entrySet()) {
                        LeadEnrichmentAttribute attr = new LeadEnrichmentAttribute();
                        attr.setFieldName(entry.getKey());
                        attr.setCustomerColumnName(entry.getValue());
                        attributes.add(attr);
                    }
                }
            }

            return attributes;
        } catch (Exception ex) {
            throw new LedpException(LedpCode.LEDP_18078, ex, new String[] { ex.getMessage() });
        }
    }

    @Override
    public Map<String, List<String>> verifyAttributes(Tenant tenant,
            List<LeadEnrichmentAttribute> attributes) {
        try {
            Map<String, List<String>> invalidFields = new HashMap<String, List<String>>();
            if (attributes == null || attributes.size() == 0) {
                return invalidFields;
            }

            String tenantName = CustomerSpace.parse(tenant.getId()).getTenantId();
            String dlUrl = tenantConfigService.getDLRestServiceAddress(tenant.getId());
            ConnectionMgr connMgr = connectionMgrFactory.getConnectionMgr(CONNECTION_MGR_TYPE,
                    tenantName, dlUrl);
            AbstractMap.SimpleImmutableEntry<String, String> typeAndVersions = lpFunctions
                    .getLPTemplateTypeAndVersion(connMgr);
            String templateType = typeAndVersions.getKey();
            Map<String, String> tablesAndProviders = getTargetTablesAndDataProviders(connMgr,
                    templateType);
            for (Entry<String, String> entry : tablesAndProviders.entrySet()) {
                String table = entry.getKey();
                String provider = entry.getValue();
                Map<String, Boolean> colsMap = getColumns(tenantName, provider, table, dlUrl);
                for (LeadEnrichmentAttribute attribute : attributes) {
                    String column = lpFunctions.getCustomerColumn(templateType, attribute.getFieldName());
                    if (!colsMap.containsKey(column.toLowerCase())) {
                        List<String> fields = invalidFields.get(table);
                        if (fields == null) {
                            fields = new ArrayList<String>();
                            invalidFields.put(table, fields);
                        }
                        fields.add(attribute.getFieldName());
                    }
                }
            }

            return invalidFields;
        } catch (Exception ex) {
            throw new LedpException(LedpCode.LEDP_18083, ex, new String[] { ex.getMessage() });
        }
    }

    private Map<String, String> getTargetTablesAndDataProviders(ConnectionMgr connMgr, String templateVersion)
            throws Exception {
        Map<String, String> tablesAndProviders = lpFunctions.getTargetTablesAndDataProviders(
                connMgr, templateVersion);
        if (tablesAndProviders == null || tablesAndProviders.size() == 0) {
            throw new LedpException(LedpCode.LEDP_18082);
        }
        return tablesAndProviders;
    }

    private Map<String, Boolean> getColumns(String tenantName, String dataProvider, String table,
            String dlUrl) {
        Map<String, Boolean> colsMap = new HashMap<String, Boolean>();
        SourceTableMetadataResult result = dataLoaderService.getSourceTableMetadata(tenantName,
                dataProvider, table, dlUrl);
        List<SourceColumnMetadata> columns = result.getMetadata();
        if (columns != null) {
            for (SourceColumnMetadata column : columns) {
                if (column != null && column.getColumnName() != null) {
                    colsMap.put(column.getColumnName().toLowerCase(), true);
                }
            }
        }
        return colsMap;
    }

    @Override
    public void saveAttributes(Tenant tenant, List<LeadEnrichmentAttribute> attributes) {
        try {
            String tenantName = CustomerSpace.parse(tenant.getId()).getTenantId();
            String dlUrl = tenantConfigService.getDLRestServiceAddress(tenant.getId());
            ConnectionMgr connMgr = connectionMgrFactory.getConnectionMgr(CONNECTION_MGR_TYPE,
                    tenantName, dlUrl);
            AbstractMap.SimpleImmutableEntry<String, String> typeAndVersions = lpFunctions
                    .getLPTemplateTypeAndVersion(connMgr);

            lpFunctions.removeLDCWritebackAttributes(connMgr, typeAndVersions.getValue());
            if (attributes != null && attributes.size() > 0) {
                Map<String, Set<String>> map = new HashMap<String, Set<String>>();
                for (LeadEnrichmentAttribute attribute : attributes) {
                    Set<String> columns = map.get(attribute.getDataSource());
                    if (columns == null) {
                        columns = new HashSet<String>();
                        map.put(attribute.getDataSource(), columns);
                    }
                    columns.add(attribute.getFieldName());
                }
                for (Entry<String, Set<String>> entry : map.entrySet()) {
                    String source = entry.getKey();
                    lpFunctions.addLDCMatch(connMgr, source, typeAndVersions.getValue());
                    lpFunctions.setLDCWritebackAttributesDefaultName(connMgr, source,
                            entry.getValue(), typeAndVersions.getKey(), typeAndVersions.getValue());
                }
            }
            connMgr.getLoadGroupMgr().commit();
        } catch (Exception ex) {
            throw new LedpException(LedpCode.LEDP_18079, ex, new String[] { ex.getMessage() });
        }
    }

    @Override
    public String getTemplateType(Tenant tenant) {
        try {
            String tenantName = CustomerSpace.parse(tenant.getId()).getTenantId();
            String dlUrl = tenantConfigService.getDLRestServiceAddress(tenant.getId());
            ConnectionMgr connMgr = connectionMgrFactory.getConnectionMgr(CONNECTION_MGR_TYPE,
                    tenantName, dlUrl);
            AbstractMap.SimpleImmutableEntry<String, String> typeAndVersions = lpFunctions
                    .getLPTemplateTypeAndVersion(connMgr);
            return typeAndVersions.getKey();
        } catch (Exception ex) {
            throw new LedpException(LedpCode.LEDP_18080, ex, new String[] { ex.getMessage() });
        }
    }

    @Override
    public int getPremiumAttributesLimitation(Tenant tenant) {
        try {
            return tenantConfigService.getMaxPremiumLeadEnrichmentAttributes(tenant.getId());
        } catch (Exception ex) {
            throw new LedpException(LedpCode.LEDP_18089, ex, new String[] { tenant.getName(), ex.getMessage() });
        }
    }
}
