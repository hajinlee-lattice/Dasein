package com.latticeengines.playmaker.service.impl;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
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
import com.latticeengines.baton.exposed.service.BatonService;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.db.exposed.util.MultiTenantContext;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.datacloud.match.MatchInput;
import com.latticeengines.domain.exposed.datacloud.match.MatchOutput;
import com.latticeengines.domain.exposed.metadata.ColumnMetadata;
import com.latticeengines.domain.exposed.metadata.InterfaceName;
import com.latticeengines.domain.exposed.playmaker.PlaymakerConstants;
import com.latticeengines.domain.exposed.playmaker.PlaymakerUtils;
import com.latticeengines.domain.exposed.propdata.manage.ColumnSelection.Predefined;
import com.latticeengines.domain.exposed.query.BusinessEntity;
import com.latticeengines.domain.exposed.query.DataPage;
import com.latticeengines.domain.exposed.query.DataRequest;
import com.latticeengines.domain.exposed.query.PageFilter;
import com.latticeengines.domain.exposed.query.frontend.FrontEndQuery;
import com.latticeengines.domain.exposed.util.AccountExtensionUtil;
import com.latticeengines.domain.exposed.util.ActivityMetricsUtils;
import com.latticeengines.playmaker.entitymgr.PlaymakerRecommendationEntityMgr;
import com.latticeengines.playmaker.service.LpiPMAccountExtension;
import com.latticeengines.playmaker.service.LpiPMPlay;
import com.latticeengines.playmakercore.service.EntityQueryGenerator;
import com.latticeengines.playmakercore.service.LpiPMRecommendation;
import com.latticeengines.proxy.exposed.cdl.LookupIdMappingProxy;
import com.latticeengines.proxy.exposed.cdl.ServingStoreProxy;
import com.latticeengines.proxy.exposed.matchapi.ColumnMetadataProxy;
import com.latticeengines.proxy.exposed.matchapi.MatchProxy;
import com.latticeengines.proxy.exposed.objectapi.EntityProxy;

import reactor.core.publisher.Flux;

@Component("lpiPMAccountExtension")
public class LpiPMAccountExtensionImpl implements LpiPMAccountExtension {

    private static final Logger log = LoggerFactory.getLogger(LpiPMAccountExtensionImpl.class);

    @Inject
    private MatchProxy matchProxy;

    @Inject
    private LpiPMPlay lpiPMPlay;

    @Inject
    private ColumnMetadataProxy columnMetadataProxy;

    @Inject
    private EntityProxy entityProxy;

    @Inject
    private LpiPMRecommendation lpiRecommendationDao;

    @Inject
    private EntityQueryGenerator entityQueryGenerator;

    @Inject
    private ServingStoreProxy servingStoreProxy;

    @Inject
    private BatonService batonService;

    @Inject
    private LookupIdMappingProxy lookupIdMappingProxy;

    private List<Predefined> filterByPredefinedSelection = //
            Collections.singletonList(Predefined.CompanyProfile);

    private static final int MAX_ROWS = 200;

    private final List<String> REQUIRED_FIELDS = Arrays.asList(InterfaceName.AccountId.name(),
            InterfaceName.SalesforceAccountID.name(), InterfaceName.CDLUpdatedTime.name());

    @Override
    public List<Map<String, Object>> getAccountExtensions(long start, long offset, long maximum,
            List<String> accountIds, String filterBy, Long recStart, String columns, boolean hasSfdcContactId,
            Map<String, String> orgInfo) {

        String customerSpace = MultiTenantContext.getCustomerSpace().toString();

        Set<String> attributes = new HashSet<>(REQUIRED_FIELDS);
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

        List<String> internalAccountIds = new ArrayList<>();
        DataPage dataPage = new DataPage();

        if (recStart == null) {
            recStart = 0L;
        }

        if (CollectionUtils.isNotEmpty(accountIds)) {
            internalAccountIds = accountIds;
        } else if (recStart > 0L) { // special case for query with
                                    // recommendation launch time.
            return getAccountExtensionsByLaunch(recStart, offset, maximum, orgInfo, customerSpace, attributes,
                    lookupIdColumn);
        } else {
            int pagesRequired = (int) Math.ceil((double) maximum / MAX_ROWS);
            for (int page = 0; page < pagesRequired; page++) {
                long currentOffset = offset + (page * MAX_ROWS);
                internalAccountIds.addAll(getInternalAccountsIdViaObjectApi(customerSpace, accountIds, lookupIdColumn,
                        start, currentOffset, Math.min(MAX_ROWS, maximum - (page * MAX_ROWS))));
            }
        }

        if (CollectionUtils.isNotEmpty(internalAccountIds)) {
            int pagesRequired = (int) Math.ceil((double) internalAccountIds.size() / MAX_ROWS);
            for (int page = 0; page < pagesRequired; page++) {
                int pageOffset = page * MAX_ROWS;
                List<String> internalAccountsIdsSublist = internalAccountIds.subList(pageOffset,
                        Math.min(pageOffset + MAX_ROWS, internalAccountIds.size()));
                DataPage currentDataPage = getAccountByIdViaMatchApi(customerSpace, internalAccountsIdsSublist,
                        attributes);
                if (currentDataPage != null) {
                    dataPage.getData().addAll(currentDataPage.getData());
                }
            }
            log.info(String.format("Accounts returned from matchapi: %s", JsonUtils.serialize(dataPage)));
        } else {
            dataPage = AccountExtensionUtil.createEmptyDataPage();
        }
        return postProcessForTimeBasedQuery(dataPage.getData(), offset, lookupIdColumn);
    }

    private List<Map<String, Object>> getAccountExtensionsByLaunch(Long recStart, long offset, long maximum,
            Map<String, String> orgInfo, String customerSpace, Set<String> attributes, String lookupIdColumn) {
        List<Map<String, Object>> accountInfos = getAccountIdsByRecommendationsInfo(false, recStart, offset, maximum,
                orgInfo);

        List<String> accountIds = new ArrayList<>();

        accountInfos.stream().forEach(info -> {
            String accountName = (String) info.get(PlaymakerConstants.AccountID);
            accountIds.add(accountName);
        });

        DataPage dataPage = new DataPage();

        if (CollectionUtils.isNotEmpty(accountIds)) {
            int pagesRequired = (int) Math.ceil((double) accountIds.size() / MAX_ROWS);
            for (int page = 0; page < pagesRequired; page++) {
                int pageOffset = page * MAX_ROWS;
                List<String> accountIdsSublist = accountIds.subList(pageOffset,
                        Math.min(pageOffset + MAX_ROWS, accountIds.size()));
                DataPage currentDataPage = getAccountByIdViaMatchApi(customerSpace, accountIdsSublist, attributes);
                if (currentDataPage != null) {
                    dataPage.getData().addAll(currentDataPage.getData());
                }
                log.info(String.format("Accounts returned from matchapi: %s", JsonUtils.serialize(dataPage)));
            }
        } else {
            dataPage = AccountExtensionUtil.createEmptyDataPage();
        }

        return postProcessForLaunchBased(dataPage.getData(), accountInfos, offset, lookupIdColumn);
    }

    @Override
    public List<Map<String, Object>> getAccountIdsByRecommendationsInfo(boolean latest, Long recStart, long offset,
            long max, Map<String, String> orgInfo) {
        List<String> launchIds = lpiPMPlay.getLaunchIdsFromDashboard(latest, recStart, null, 0, orgInfo);
        return lpiRecommendationDao.getAccountIdsFromRecommendationByLaunchId(launchIds, recStart, (int) offset,
                (int) max);
    }

    private void setPageFilter(FrontEndQuery frontEndQuery, Long offset, Long maximum) {
        offset = (offset == null) ? 0 : offset;
        maximum = (maximum == null) ? MAX_ROWS : maximum;
        PageFilter pageFilter = new PageFilter(offset, Math.min(MAX_ROWS, maximum));
        frontEndQuery.setPageFilter(pageFilter);
    }

    private List<Map<String, Object>> postProcessForTimeBasedQuery(List<Map<String, Object>> data, long offset,
            String lookupIdColumn) {

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

    private List<Map<String, Object>> postProcessForLaunchBased(List<Map<String, Object>> data,
            List<Map<String, Object>> accountInfos, long offset, String lookupIdColumn) {

        if (CollectionUtils.isNotEmpty(data)) {
            long rowNum = offset + 1;

            Map<String, Object> lastModifyTimeLookups = new HashMap<>();
            accountInfos.stream().forEach(info -> {
                lastModifyTimeLookups.put((String) info.get(PlaymakerConstants.AccountID),
                        info.get(PlaymakerConstants.LastModificationDate));
            });

            for (Map<String, Object> accExtRec : data) {
                if (accExtRec.containsKey(InterfaceName.AccountId.name())) {
                    accExtRec.put(PlaymakerConstants.ID, accExtRec.get(InterfaceName.AccountId.name()));
                    accExtRec.put(PlaymakerConstants.LEAccountExternalID,
                            accExtRec.get(InterfaceName.AccountId.name()));

                    if (lastModifyTimeLookups.containsKey(accExtRec.get(InterfaceName.AccountId.name()))) {
                        accExtRec.put(PlaymakerRecommendationEntityMgr.RECOMMENDATION_DATE,
                                lastModifyTimeLookups.get(accExtRec.get(InterfaceName.AccountId.name())));
                    }
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

    private List<String> getInternalAccountsIdViaObjectApi(String customerSpace, List<String> accountIds,
            String lookupIdColumn, Long start, Long offset, Long maximum) {

        DataPage entityData;

        try {
            FrontEndQuery frontEndQuery = AccountExtensionUtil.constructFrontEndQuery(customerSpace, accountIds,
                    lookupIdColumn, start, true, batonService.isEntityMatchEnabled(CustomerSpace.parse(customerSpace)));
            setPageFilter(frontEndQuery, offset, maximum);
            log.info(String.format("Calling entityProxy with request payload: %s", JsonUtils.serialize(frontEndQuery)));
            entityData = entityProxy.getDataFromObjectApi(customerSpace, frontEndQuery);
        } catch (Exception e) {
            FrontEndQuery frontEndQuery = AccountExtensionUtil.constructFrontEndQuery(customerSpace, accountIds,
                    lookupIdColumn, start, false,
                    batonService.isEntityMatchEnabled(CustomerSpace.parse(customerSpace)));
            setPageFilter(frontEndQuery, offset, maximum);
            log.info(String.format("Calling entityProxy with request payload: %s", JsonUtils.serialize(frontEndQuery)));
            entityData = entityProxy.getDataFromObjectApi(customerSpace, frontEndQuery);
        }

        return AccountExtensionUtil.extractAccountIds(entityData);
    }

    private DataPage getAccountByIdViaMatchApi(String customerSpace, List<String> internalAccountIds,
            Set<String> attributes) {

        String dataCloudVersion = columnMetadataProxy.latestVersion(null).getVersion();
        MatchInput matchInput;
        if (batonService.isEntityMatchEnabled(CustomerSpace.parse(customerSpace))) {
            matchInput = AccountExtensionUtil.constructEntityMatchInput(customerSpace, internalAccountIds, attributes,
                    dataCloudVersion);
        } else {
            matchInput = AccountExtensionUtil.constructMatchInput(customerSpace, internalAccountIds, attributes,
                    dataCloudVersion);
        }

        log.info(String.format("Calling matchapi with request payload: %s", JsonUtils.serialize(matchInput)));

        MatchOutput matchOutput = matchProxy.matchRealTime(matchInput);

        return AccountExtensionUtil.convertToDataPage(matchOutput);
    }

    @Override
    public long getAccountExtensionCount(long start, List<String> accountIds, String filterBy, Long recStart,
            Map<String, String> orgInfo) {

        if (CollectionUtils.isNotEmpty(accountIds)) {
            return accountIds.size();
        }
        if (recStart != null) {
            if (recStart > 0L) {
                List<String> launchIds = lpiPMPlay.getLaunchIdsFromDashboard(true, recStart, null, 0, orgInfo);
                if (CollectionUtils.isNotEmpty(launchIds)) {
                    return lpiRecommendationDao.getAccountIdsCountFromRecommendationByLaunchId(launchIds, recStart);
                } else {
                    return 0;
                }
            }
        }

        String customerSpace = MultiTenantContext.getCustomerSpace().toString();
        DataRequest dataRequest = new DataRequest();
        dataRequest.setAccountIds(accountIds);

        FrontEndQuery frontEndQuery = entityQueryGenerator.generateEntityQuery(start, dataRequest);
        log.info(String.format("Calling entityProxy with request payload: %s", JsonUtils.serialize(frontEndQuery)));
        return entityProxy.getCountFromObjectApi(customerSpace, frontEndQuery);
    }

    @Override
    public List<Map<String, Object>> getAccountExtensionSchema(String customerSpace) {
        List<Map<String, Object>> result = new ArrayList<Map<String, Object>>();
        BusinessEntity.COMPANY_PROFILE_ENTITIES.forEach(entity -> {
            result.addAll(getSchema(customerSpace, entity));
        });
        return result;
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
        if (CollectionUtils.isNotEmpty(cms)) {
            if (BusinessEntity.PurchaseHistory.equals(entity)) {
                DataPage dataPage = entityProxy.getProducts(customerSpace);
                Map<String, String> productNameMap = new HashMap<>();
                if (dataPage != null && CollectionUtils.isNotEmpty(dataPage.getData())) {
                    dataPage.getData().forEach(map -> productNameMap.put( //
                            map.get(InterfaceName.ProductId.name()).toString(), //
                            map.get(InterfaceName.ProductName.name()).toString() //
                    ));
                }
                cms.forEach(cm -> {
                    String productId = ActivityMetricsUtils.getProductIdFromFullName(cm.getAttrName());
                    String productName = productNameMap.get(productId);
                    cm.setDisplayName(productName + ": " + cm.getDisplayName());
                });
            }
            Flux<Map<String, Object>> flux = Flux.fromIterable(cms) //
                    .map(metadata -> {
                        Map<String, Object> metadataInfoMap = new HashMap<>();
                        metadataInfoMap.put(PlaymakerConstants.DisplayName, metadata.getDisplayName());
                        metadataInfoMap.put(PlaymakerConstants.Type, PlaymakerUtils.convertToSFDCFieldType(
                                metadata.isDateAttribute() ? PlaymakerConstants.DateTime : metadata.getJavaClass()));
                        metadataInfoMap.put(PlaymakerConstants.StringLength,
                                PlaymakerUtils.findLengthIfStringType(metadata.getJavaClass()));
                        metadataInfoMap.put(PlaymakerConstants.Field, metadata.getAttrName());
                        return metadataInfoMap;
                    }) //
                    .sort(Comparator.comparing(a -> ((String) a.get(PlaymakerConstants.Field))));
            return flux.collectList().block();
        } else {
            return new ArrayList<>();
        }
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

    @VisibleForTesting
    void setColumnMetadataProxy(ColumnMetadataProxy columnMetadataProxy) {
        this.columnMetadataProxy = columnMetadataProxy;
    }

    @VisibleForTesting
    void setMatchProxy(MatchProxy matchProxy) {
        this.matchProxy = matchProxy;
    }

    @VisibleForTesting
    void setBatonService(BatonService batonService) {
        this.batonService = batonService;
    }
}
