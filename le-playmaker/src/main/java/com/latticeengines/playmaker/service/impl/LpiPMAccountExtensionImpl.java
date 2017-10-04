package com.latticeengines.playmaker.service.impl;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.StringTokenizer;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.google.common.annotations.VisibleForTesting;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.metadata.Attribute;
import com.latticeengines.domain.exposed.metadata.InterfaceName;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.metadata.TableRoleInCollection;
import com.latticeengines.domain.exposed.playmaker.PlaymakerConstants;
import com.latticeengines.domain.exposed.playmaker.PlaymakerUtils;
import com.latticeengines.domain.exposed.query.DataPage;
import com.latticeengines.domain.exposed.query.DataRequest;
import com.latticeengines.domain.exposed.query.frontend.FrontEndQuery;
import com.latticeengines.playmaker.entitymgr.PlaymakerRecommendationEntityMgr;
import com.latticeengines.playmaker.service.LpiPMAccountExtension;
import com.latticeengines.playmakercore.service.EntityQueryGenerator;
import com.latticeengines.proxy.exposed.metadata.DataCollectionProxy;
import com.latticeengines.proxy.exposed.objectapi.EntityProxy;
import com.latticeengines.security.exposed.util.MultiTenantContext;

@Component("lpiPMAccountExtension")
public class LpiPMAccountExtensionImpl implements LpiPMAccountExtension {

    private static final Logger log = LoggerFactory.getLogger(LpiPMAccountExtensionImpl.class);

    @Autowired
    private EntityProxy entityProxy;

    @Autowired
    private EntityQueryGenerator entityQueryGenerator;

    @Autowired
    private DataCollectionProxy dataCollectionProxy;

    @Override
    public List<Map<String, Object>> getAccountExtensions(long start, long offset, long maximum,
            List<String> accountIds, Long recStart, String columns, boolean hasSfdcContactId) {
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

        FrontEndQuery frontEndQuery = entityQueryGenerator.generateEntityQuery(start, offset, maximum, dataRequest);

        log.info(String.format("Calling entityProxy with request payload: %s", JsonUtils.serialize(frontEndQuery)));
        DataPage dataPage = entityProxy.getData(customerSpace, frontEndQuery);

        return postProcess(dataPage.getData(), offset);
    }

    private List<Map<String, Object>> postProcess(List<Map<String, Object>> data, long offset) {

        if (CollectionUtils.isNotEmpty(data)) {
            long rowNum = offset + 1;

            for (Map<String, Object> accExtRec : data) {
                if (accExtRec.containsKey(InterfaceName.AccountId.name())) {
                    accExtRec.put(PlaymakerConstants.ID, accExtRec.get(InterfaceName.AccountId.name()));
                }
                if (accExtRec.containsKey(InterfaceName.SalesforceAccountID.name())) {
                    accExtRec.put(PlaymakerConstants.SfdcAccountID,
                            accExtRec.get(InterfaceName.SalesforceAccountID.name()));
                }
                if (accExtRec.containsKey(InterfaceName.LatticeAccountId.name())) {
                    accExtRec.put(PlaymakerConstants.LEAccountExternalID,
                            accExtRec.get(InterfaceName.LatticeAccountId.name()));
                }

                accExtRec.put(PlaymakerRecommendationEntityMgr.LAST_MODIFIATION_DATE_KEY,
                        accExtRec.get(InterfaceName.LastModifiedDate.name()));

                accExtRec.put(PlaymakerConstants.RowNum, rowNum++);
            }
        }

        return data;
    }

    @Override
    public long getAccountExtensionCount(long start, List<String> accountIds, Long recStart) {
        String customerSpace = MultiTenantContext.getCustomerSpace().toString();
        DataRequest dataRequest = new DataRequest();
        dataRequest.setAccountIds(accountIds);

        FrontEndQuery frontEndQuery = entityQueryGenerator.generateEntityQuery(start, dataRequest);
        log.info(String.format("Calling entityProxy with request payload: %s", JsonUtils.serialize(frontEndQuery)));
        return entityProxy.getCount(customerSpace, frontEndQuery);
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
                    metadataInfoMap.put(PlaymakerConstants.DisplayName, metadata.getDisplayName());
                    metadataInfoMap.put(PlaymakerConstants.Type,
                            PlaymakerUtils.convertToSFDCFieldType(metadata.getSourceLogicalDataType()));
                    metadataInfoMap.put(PlaymakerConstants.StringLength,
                            PlaymakerUtils.findLengthIfStringType(metadata.getSourceLogicalDataType()));
                    metadataInfoMap.put(PlaymakerConstants.Field, metadata.getName());
                    return metadataInfoMap;
                });

        return stream.collect(Collectors.toList());
    }

    private List<Attribute> getSchemaAttributes(TableRoleInCollection role) {
        String customerSpace = MultiTenantContext.getCustomerSpace().toString();
        Table schemaTable = dataCollectionProxy.getTable(customerSpace, role);
        List<Attribute> schemaAttributes = schemaTable.getAttributes();
        return schemaAttributes;
    }

    @VisibleForTesting
    void setEntityProxy(EntityProxy entityProxy) {
        this.entityProxy = entityProxy;
    }

    @VisibleForTesting
    void setEntityQueryGenerator(EntityQueryGenerator entityQueryGenerator) {
        this.entityQueryGenerator = entityQueryGenerator;
    }

}
