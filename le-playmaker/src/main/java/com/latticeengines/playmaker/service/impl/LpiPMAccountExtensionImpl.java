package com.latticeengines.playmaker.service.impl;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.StringTokenizer;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.commons.lang.StringUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.latticeengines.common.exposed.util.DateTimeUtils;
import com.latticeengines.domain.exposed.metadata.Attribute;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.metadata.TableRoleInCollection;
import com.latticeengines.domain.exposed.query.DataPage;
import com.latticeengines.domain.exposed.query.DataRequest;
import com.latticeengines.playmaker.service.LpiPMAccountExtension;
import com.latticeengines.proxy.exposed.metadata.DataCollectionProxy;
import com.latticeengines.proxy.exposed.objectapi.AccountProxy;
import com.latticeengines.security.exposed.util.MultiTenantContext;

@Component("lpiPMAccountExtension")
public class LpiPMAccountExtensionImpl implements LpiPMAccountExtension {

    private String VAR_CHAR = "varchar";

    @Autowired
    private AccountProxy accountProxy;

    @Autowired
    private DataCollectionProxy dataCollectionProxy;

    @Override
    public List<Map<String, Object>> getAccountExtensions(long start, int offset, int maximum, List<String> accountIds,
            Long recStart, String columns, boolean hasSfdcContactId) {
        String customerSpace = MultiTenantContext.getCustomerSpace().toString();
        DataRequest dataRequest = new DataRequest();
        dataRequest.setAccountIds(accountIds);
        List<String> attributes = new ArrayList<>();
        if (StringUtils.isNotBlank(columns)) {
            StringTokenizer tkz = new StringTokenizer(columns, ",");
            while (tkz.hasMoreTokens()) {
                attributes.add(tkz.nextToken().trim());
            }
        }

        dataRequest.setAttributes(attributes);
        DataPage dataPage = accountProxy.getAccounts(customerSpace,
                DateTimeUtils.convertToStringUTCISO8601(LpiPMUtils.dateFromEpochSeconds(start)), offset, maximum,
                dataRequest);

        return dataPage.getData();
    }

    @Override
    public int getAccountExtensionCount(long start, List<String> accountIds, Long recStart) {
        String customerSpace = MultiTenantContext.getCustomerSpace().toString();
        DataRequest dataRequest = new DataRequest();
        dataRequest.setAccountIds(accountIds);
        return (int) accountProxy.getAccountsCount(customerSpace,
                DateTimeUtils.convertToStringUTCISO8601(LpiPMUtils.dateFromEpochSeconds(start)), dataRequest);
    }

    @Override
    public List<Map<String, Object>> getAccountExtensionSchema() {
        return getSchema(TableRoleInCollection.BucketedAccount);
    }

    @Override
    public List<Map<String, Object>> getContactExtensionSchema() {
        return getSchema(TableRoleInCollection.SortedContact);
    }

    @Override
    public int getAccountExtensionColumnCount() {
        List<Attribute> schemaAttributes = getSchemaAttributes(TableRoleInCollection.BucketedAccount);
        return schemaAttributes.size();
    }

    @Override
    public int getContactExtensionColumnCount() {
        List<Attribute> schemaAttributes = getSchemaAttributes(TableRoleInCollection.SortedContact);
        return schemaAttributes.size();
    }

    private List<Map<String, Object>> getSchema(TableRoleInCollection role) {
        List<Attribute> schemaAttributes = getSchemaAttributes(role);

        Stream<Map<String, Object>> stream = schemaAttributes.stream() //
                .sorted(Comparator.comparing(Attribute::getName)) //
                .map(metadata -> {
                    Map<String, Object> metadataInfoMap = new HashMap<>();
                    metadataInfoMap.put("DisplayName", metadata.getDisplayName());
                    metadataInfoMap.put("Type", convertToSFDCFieldType(metadata.getSourceLogicalDataType()));
                    metadataInfoMap.put("JavaType", metadata.getPhysicalDataType());
                    metadataInfoMap.put("StringLength", findLengthIfStringType(metadata.getSourceLogicalDataType()));
                    metadataInfoMap.put("Field", metadata.getName());
                    return metadataInfoMap;
                });

        return stream.collect(Collectors.toList());
    }

    private String convertToSFDCFieldType(String sourceLogicalDataType) {
        String type = sourceLogicalDataType;

        if (StringUtils.isNotBlank(sourceLogicalDataType)) {
            sourceLogicalDataType = sourceLogicalDataType.toLowerCase();

            if (sourceLogicalDataType.contains(VAR_CHAR)) {
                type = "nvarchar";
            } else if (sourceLogicalDataType.equals("double")) {
                type = "decimal";
            } else if (sourceLogicalDataType.equals("long")) {
                type = "bigint";
            } else if (sourceLogicalDataType.equals("boolean")) {
                type = "bit";
            }
        } else {
            type = "";
        }

        return type;
    }

    private Integer findLengthIfStringType(String sourceLogicalDataType) {
        Integer length = null;

        if (StringUtils.isNotBlank(sourceLogicalDataType)) {
            sourceLogicalDataType = sourceLogicalDataType.toLowerCase();

            if (sourceLogicalDataType.contains(VAR_CHAR)) {
                length = 4000;

                if (sourceLogicalDataType.contains("(")) {

                    sourceLogicalDataType = sourceLogicalDataType.substring(sourceLogicalDataType.indexOf("("));

                    if (sourceLogicalDataType.contains(")")) {

                        sourceLogicalDataType = sourceLogicalDataType.substring(0, sourceLogicalDataType.indexOf(")"));

                        if (StringUtils.isNumeric(sourceLogicalDataType)) {
                            length = Integer.parseInt(sourceLogicalDataType);
                        }
                    }
                }
            }
        }

        return length;
    }

    private List<Attribute> getSchemaAttributes(TableRoleInCollection role) {
        String customerSpace = MultiTenantContext.getCustomerSpace().toString();
        Table schemaTable = dataCollectionProxy.getTable(customerSpace, role);
        List<Attribute> schemaAttributes = schemaTable.getAttributes();
        return schemaAttributes;
    }

}
