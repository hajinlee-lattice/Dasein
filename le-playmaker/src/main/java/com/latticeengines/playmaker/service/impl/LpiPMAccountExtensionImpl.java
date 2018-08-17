package com.latticeengines.playmaker.service.impl;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.StringTokenizer;

import javax.inject.Inject;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import com.google.common.annotations.VisibleForTesting;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.db.exposed.util.MultiTenantContext;
import com.latticeengines.domain.exposed.metadata.ColumnMetadata;
import com.latticeengines.domain.exposed.metadata.InterfaceName;
import com.latticeengines.domain.exposed.playmaker.PlaymakerConstants;
import com.latticeengines.domain.exposed.playmaker.PlaymakerUtils;
import com.latticeengines.domain.exposed.propdata.manage.ColumnSelection;
import com.latticeengines.domain.exposed.propdata.manage.ColumnSelection.Predefined;
import com.latticeengines.domain.exposed.query.BusinessEntity;
import com.latticeengines.domain.exposed.query.DataPage;
import com.latticeengines.domain.exposed.query.DataRequest;
import com.latticeengines.domain.exposed.query.frontend.FrontEndQuery;
import com.latticeengines.playmaker.entitymgr.PlaymakerRecommendationEntityMgr;
import com.latticeengines.playmaker.service.LpiPMAccountExtension;
import com.latticeengines.playmakercore.service.EntityQueryGenerator;
import com.latticeengines.proxy.exposed.cdl.LookupIdMappingProxy;
import com.latticeengines.proxy.exposed.cdl.ServingStoreProxy;
import com.latticeengines.proxy.exposed.objectapi.EntityProxy;

import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

@Component("lpiPMAccountExtension")
public class LpiPMAccountExtensionImpl implements LpiPMAccountExtension {

    private static final Logger log = LoggerFactory.getLogger(LpiPMAccountExtensionImpl.class);

    @Inject
    private EntityProxy entityProxy;

    @Inject
    private EntityQueryGenerator entityQueryGenerator;

    @Inject
    private ServingStoreProxy servingStoreProxy;

    @Inject
    private LookupIdMappingProxy lookupIdMappingProxy;

    private List<Predefined> filterByPredefinedSelection = //
            Collections.singletonList(Predefined.CompanyProfile);

    @Override
    public List<Map<String, Object>> getAccountExtensions(long start, long offset, long maximum,
            List<String> accountIds, Long recStart, String columns, boolean hasSfdcContactId,
            Map<String, String> orgInfo) {
        String customerSpace = MultiTenantContext.getCustomerSpace().toString();
        DataRequest dataRequest = new DataRequest();
        dataRequest.setAccountIds(accountIds);
        Set<String> attributes = new HashSet<>();
        if (StringUtils.isNotBlank(columns)) {
            StringTokenizer tkz = new StringTokenizer(columns, ",");
            while (tkz.hasMoreTokens()) {
                attributes.add(tkz.nextToken().trim());
            }
        }

        String lookupIdColumn = lookupIdMappingProxy.findLookupIdColumn(orgInfo, customerSpace);
        lookupIdColumn = lookupIdColumn == null ? null : lookupIdColumn.trim();
        if (StringUtils.isNotBlank(lookupIdColumn)) {
            attributes.add(lookupIdColumn);
        }

        dataRequest.setAttributes(new ArrayList<>(attributes));

        FrontEndQuery frontEndQuery = entityQueryGenerator.generateEntityQuery(start, offset, maximum, dataRequest);

        log.info(String.format("Calling entityProxy with request payload: %s", JsonUtils.serialize(frontEndQuery)));
        DataPage dataPage = entityProxy.getDataFromObjectApi(customerSpace, frontEndQuery, null, true);

        return postProcess(dataPage.getData(), offset, lookupIdColumn);
    }

    private List<Map<String, Object>> postProcess(List<Map<String, Object>> data, long offset, String lookupIdColumn) {

        if (CollectionUtils.isNotEmpty(data)) {
            long rowNum = offset + 1;

            for (Map<String, Object> accExtRec : data) {
                if (accExtRec.containsKey(InterfaceName.AccountId.name())) {
                    accExtRec.put(PlaymakerConstants.ID, accExtRec.get(InterfaceName.AccountId.name()));
                    accExtRec.put(PlaymakerConstants.LEAccountExternalID,
                            accExtRec.get(InterfaceName.AccountId.name()));
                }

                if (StringUtils.isNotBlank(lookupIdColumn) && accExtRec.containsKey(lookupIdColumn)) {
                    accExtRec.put(PlaymakerConstants.SfdcAccountID, accExtRec.get(lookupIdColumn));
                }

                accExtRec.put(PlaymakerRecommendationEntityMgr.LAST_MODIFIATION_DATE_KEY,
                        accExtRec.get(InterfaceName.CDLUpdatedTime.name()));

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
        return entityProxy.getCountFromObjectApi(customerSpace, frontEndQuery);
    }

    @Override
    public List<Map<String, Object>> getAccountExtensionSchema(String customerSpace) {
        return getSchema(customerSpace, BusinessEntity.Account);
    }

    @Override
    public List<Map<String, Object>> getContactExtensionSchema(String customerSpace) {
        return getSchema(customerSpace, BusinessEntity.Contact);
    }

    @Override
    public int getAccountExtensionColumnCount(String customerSpace) {
        List<ColumnMetadata> cms = servingStoreProxy
                .getDecoratedMetadata(customerSpace, BusinessEntity.Account, filterByPredefinedSelection).collectList()
                .block();

        return cms == null ? 0 : cms.size();
    }

    @Override
    public int getContactExtensionColumnCount(String customerSpace) {
        List<ColumnMetadata> cms = servingStoreProxy
                .getDecoratedMetadata(customerSpace, BusinessEntity.Contact, filterByPredefinedSelection).collectList()
                .block();

        return cms == null ? 0 : cms.size();
    }

    private List<Map<String, Object>> getSchema(String customerSpace, BusinessEntity entity) {
        List<ColumnMetadata> cms = servingStoreProxy
                .getDecoratedMetadata(customerSpace, entity, filterByPredefinedSelection).collectList().block();

        Mono<List<Map<String, Object>>> stream = Flux.fromIterable(cms) //
                .map(metadata -> {
                    Map<String, Object> metadataInfoMap = new HashMap<>();
                    metadataInfoMap.put(PlaymakerConstants.DisplayName, metadata.getDisplayName());
                    metadataInfoMap.put(PlaymakerConstants.Type,
                            PlaymakerUtils.convertToSFDCFieldType(metadata.getJavaClass()));
                    metadataInfoMap.put(PlaymakerConstants.StringLength,
                            PlaymakerUtils.findLengthIfStringType(metadata.getJavaClass()));
                    metadataInfoMap.put(PlaymakerConstants.Field, metadata.getAttrName());
                    return metadataInfoMap;
                }) //
                .collectSortedList((a, b) -> ((String) a.get(PlaymakerConstants.Field))
                        .compareTo(((String) b.get(PlaymakerConstants.Field))));

        return stream.block();
    }

    @VisibleForTesting
    void setEntityProxy(EntityProxy entityProxy) {
        this.entityProxy = entityProxy;
    }

    @VisibleForTesting
    void setEntityQueryGenerator(EntityQueryGenerator entityQueryGenerator) {
        this.entityQueryGenerator = entityQueryGenerator;
    }

    @VisibleForTesting
    void setLookupIdMappingProxy(LookupIdMappingProxy lookupIdMappingProxy) {
        this.lookupIdMappingProxy = lookupIdMappingProxy;
    }
}
