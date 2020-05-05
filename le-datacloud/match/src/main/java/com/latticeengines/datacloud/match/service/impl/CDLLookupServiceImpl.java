package com.latticeengines.datacloud.match.service.impl;

import static com.latticeengines.domain.exposed.metadata.TableRoleInCollection.ConsolidatedAccount;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import javax.inject.Inject;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.collections4.MapUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.LoadingCache;
import com.google.common.annotations.VisibleForTesting;
import com.latticeengines.datacloud.match.service.CDLLookupService;
import com.latticeengines.datafabric.entitymanager.GenericTableEntityMgr;
import com.latticeengines.datafabric.entitymanager.impl.GenericTableEntityMgrImpl;
import com.latticeengines.datafabric.service.datastore.FabricDataService;
import com.latticeengines.datafabric.service.message.FabricMessageService;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.datacloud.match.MatchInput;
import com.latticeengines.domain.exposed.metadata.ColumnMetadata;
import com.latticeengines.domain.exposed.metadata.DataCollection;
import com.latticeengines.domain.exposed.metadata.InterfaceName;
import com.latticeengines.domain.exposed.metadata.TableRoleInCollection;
import com.latticeengines.domain.exposed.metadata.datastore.DynamoDataUnit;
import com.latticeengines.domain.exposed.propdata.manage.ColumnSelection;
import com.latticeengines.domain.exposed.query.BusinessEntity;
import com.latticeengines.domain.exposed.serviceapps.core.AttrState;
import com.latticeengines.proxy.exposed.cdl.DataCollectionProxy;
import com.latticeengines.proxy.exposed.cdl.ServingStoreProxy;

import reactor.core.publisher.Flux;

@Service("CDLLookupService")
public class CDLLookupServiceImpl implements CDLLookupService {

    private static final Logger log = LoggerFactory.getLogger(CDLLookupServiceImpl.class);

    @Inject
    private ServingStoreProxy servingStoreProxy;

    @Inject
    private DataCollectionProxy dataCollectionProxy;

    @Inject
    private FabricMessageService messageService;

    @Inject
    private FabricDataService dataService;

    private final Map<String, GenericTableEntityMgr> dynamoDataStoreMap = new ConcurrentHashMap<>();

    private LoadingCache<String, DynamoDataUnit> roleBasedDynamoDataCache = Caffeine.newBuilder() //
            .maximumSize(1000) //
            .expireAfterWrite(1, TimeUnit.MINUTES) //
            .build(this::getDynamoDataUnit);

    private LoadingCache<String, List<DynamoDataUnit>> dataUnitsCache = Caffeine.newBuilder() //
            .maximumSize(500) //
            .expireAfterWrite(1, TimeUnit.MINUTES) //
            .build(this::getCustomDynamoDataUnits);

    @Override
    public List<ColumnMetadata> parseMetadata(MatchInput input) {
        List<ColumnSelection.Predefined> predefinedList = Collections.emptyList();
        Set<String> extraColumns = Collections.emptySet();
        boolean isCustomSelection = false;
        if (input.getUnionSelection() != null) {
            if (MapUtils.isNotEmpty(input.getUnionSelection().getPredefinedSelections())) {
                predefinedList = new ArrayList<>(input.getUnionSelection().getPredefinedSelections().keySet());
            }
            if (input.getUnionSelection().getCustomSelection() != null) {
                extraColumns = extractColumnNames(input.getUnionSelection().getCustomSelection());
            }
        } else if (input.getPredefinedSelection() != null) {
            predefinedList = Collections.singletonList(input.getPredefinedSelection());
        } else {
            extraColumns = extractColumnNames(input.getCustomSelection());
            isCustomSelection = true;
        }

        String customerSpace = input.getTenant().getId();
        @SuppressWarnings("unused")
        DataCollection.Version version = input.getDataCollectionVersion();
        // TODO: get metadata by version
        List<ColumnMetadata> cms = new ArrayList<>();
        for (BusinessEntity entity : BusinessEntity.ACCOUNT_MATCH_ENTITIES) {
            List<ColumnMetadata> list = servingStoreProxy.getDecoratedMetadataFromCache(customerSpace, entity);
            if (CollectionUtils.isNotEmpty(list)) {
                cms.addAll(list);
            }
        }
        boolean finalIsCustomSelection = isCustomSelection;
        Flux<ColumnMetadata> flux = Flux.fromIterable(cms) //
                .filter(cm -> finalIsCustomSelection || !AttrState.Inactive.equals(cm.getAttrState()));

        final List<ColumnSelection.Predefined> finalPredefinedList = new ArrayList<>(predefinedList);
        final Set<String> finalExtraColumns = new HashSet<>(extraColumns);
        final Set<String> outputColumnNames = new HashSet<>();
        flux = flux.filter(cm -> {
            boolean alreadyIncluded = outputColumnNames.contains(cm.getAttrName());
            boolean shouldAdd = false;
            if (!alreadyIncluded) {
                boolean inExtraColumnList = finalExtraColumns.contains(cm.getAttrName());
                boolean inPredefinedList = false;
                if (CollectionUtils.isNotEmpty(finalPredefinedList)) {
                    for (ColumnSelection.Predefined predefined : finalPredefinedList) {
                        if (cm.isEnabledFor(predefined)) {
                            inPredefinedList = true;
                            break;
                        }
                    }
                }
                shouldAdd = inExtraColumnList || inPredefinedList;
                if (shouldAdd) {
                    outputColumnNames.add(cm.getAttrName());
                }
            }
            return shouldAdd;
        });

        return flux.collectList().block();
    }

    @Override
    public DynamoDataUnit parseAccountLookupDataUnit(MatchInput input) {
        String customerSpace = input.getTenant().getId();
        DataCollection.Version version = input.getDataCollectionVersion();
        return roleBasedDynamoDataCache.get(getCacheKey(customerSpace, TableRoleInCollection.AccountLookup, version));
    }

    @Override
    public List<DynamoDataUnit> parseCustomDynamoDataUnits(MatchInput input) {
        String customerSpace = input.getTenant().getId();
        DataCollection.Version version = input.getDataCollectionVersion();
        return dataUnitsCache.get(version == null ? customerSpace : customerSpace + "|" + version.toString());
    }

    @Override
    public String lookupInternalAccountId(String customerSpace, DataCollection.Version version, String lookupIdKey,
            String lookupIdValue) {
        DynamoDataUnit lookupDataUnit = //
                roleBasedDynamoDataCache.get(getCacheKey(CustomerSpace.shortenCustomerSpace(customerSpace),
                        TableRoleInCollection.AccountLookup, version));
        return lookupInternalAccountId(lookupDataUnit, lookupIdKey, lookupIdValue);
    }

    @Override
    public List<Map<String, Object>> lookupContactsByInternalAccountId(String customerSpace,
            DataCollection.Version version, String lookupIdKey, String lookupIdValue, String contactId) {
        String internalAccountId = lookupInternalAccountId(customerSpace, version, lookupIdKey, lookupIdValue);

        if (StringUtils.isBlank(internalAccountId)) {
            log.info(String.format("No Account found for LookupId:%s | LookupIdValue:%s | CustomerSpace: %s",
                    lookupIdKey, lookupIdValue, customerSpace));
            return new ArrayList<>();
        }

        DynamoDataUnit consolidatedContactLookupDataUnit = roleBasedDynamoDataCache.get(getCacheKey(
                CustomerSpace.shortenCustomerSpace(customerSpace), TableRoleInCollection.ConsolidatedContact, version));
        if (consolidatedContactLookupDataUnit == null) {
            log.info(String.format("No Dynamo dataunit found for Account based contact lookups for CustomerSpace: %s",
                    customerSpace));
            return new ArrayList<>();
        }

        String signature = consolidatedContactLookupDataUnit.getSignature();
        GenericTableEntityMgr tableEntityMgr = getTableEntityMgr(signature);
        String tenantId = parseTenantId(consolidatedContactLookupDataUnit);
        String tableName = parseTableName(consolidatedContactLookupDataUnit);

        List<Map<String, Object>> contactData = StringUtils.isBlank(contactId)
                ? tableEntityMgr.getAllByPartitionKey(tenantId, tableName, internalAccountId)
                : Collections.singletonList(
                        tableEntityMgr.getByKeyPair(tenantId, tableName, Pair.of(internalAccountId, contactId)));

        DynamoDataUnit curatedContactLookupDataUnit = roleBasedDynamoDataCache
                .get(getCacheKey(CustomerSpace.shortenCustomerSpace(customerSpace),
                        TableRoleInCollection.CalculatedCuratedContact, version));
        if (curatedContactLookupDataUnit != null) {
            log.info(String.format("Curated contact data found for CustomerSpace: %s", customerSpace));
            signature = curatedContactLookupDataUnit.getSignature();
            tableEntityMgr = getTableEntityMgr(signature);
            tenantId = parseTenantId(curatedContactLookupDataUnit);
            tableName = parseTableName(curatedContactLookupDataUnit);

            List<Map<String, Object>> curatedContactData = StringUtils.isBlank(contactId)
                    ? tableEntityMgr.getAllByPartitionKey(tenantId, tableName, internalAccountId)
                    : Collections.singletonList(
                            tableEntityMgr.getByKeyPair(tenantId, tableName, Pair.of(internalAccountId, contactId)));
            return merge(contactData, curatedContactData);
        } else {
            log.info(String.format("No Dynamo dataunit found for Curated contact data for CustomerSpace: %s",
                    customerSpace));
        }

        return contactData;
    }

    @SuppressWarnings("SuspiciousMethodCalls")
    @VisibleForTesting
    List<Map<String, Object>> merge(List<Map<String, Object>> contactData,
            List<Map<String, Object>> curatedContactData) {
        if (CollectionUtils.isEmpty(curatedContactData))
            return contactData;

        String contactId = InterfaceName.ContactId.name();
        Map<String, Map<String, Object>> mappedData = curatedContactData.stream()
                .collect(Collectors.toMap(c -> (String) c.get(contactId), c -> c));
        for (Map<String, Object> contact : contactData) {
            if (mappedData.containsKey(contact.get(contactId))) {
                contact.putAll(mappedData.get(contact.get(contactId)));
            }
        }
        return contactData;
    }

    @Override
    public Map<String, Object> lookup(DynamoDataUnit lookupDataUnit, List<DynamoDataUnit> dynamoDataUnits,
            String lookupIdKey, String lookupIdValue) {
        if (!InterfaceName.AccountId.name().equals(lookupIdKey)) {
            if (lookupDataUnit != null) {
                String accountId = lookupInternalAccountId(lookupDataUnit, lookupIdKey, lookupIdValue);
                if (accountId == null) {
                    throw new RuntimeException(String.format("Failed to find account by lookupId %s=%s", //
                            lookupIdKey, lookupIdValue));
                }
            } else {
                throw new UnsupportedOperationException("Only support lookup by AccountId.");
            }
        }
        Pair<String, String> keyPair = Pair.of(lookupIdValue, "0");
        Map<String, Object> data = new HashMap<>();
        if (CollectionUtils.isEmpty(dynamoDataUnits)) {
            log.info("No dynamo data unit found for custom account.");

        } else {
            // signature -> (tenantId -> [table names])
            Map<String, Map<String, List<String>>> map = new HashMap<>();
            dynamoDataUnits.forEach(dynamoDataUnit -> {
                if (!map.containsKey(dynamoDataUnit.getSignature())) {
                    map.put(dynamoDataUnit.getSignature(), new HashMap<>());
                }
                String tenantId = parseTenantId(dynamoDataUnit);
                if (!map.get(dynamoDataUnit.getSignature()).containsKey(tenantId)) {
                    map.get(dynamoDataUnit.getSignature()).put(tenantId, new ArrayList<>());
                }
                String tableName = parseTableName(dynamoDataUnit);
                map.get(dynamoDataUnit.getSignature()).get(tenantId).add(tableName);
            });
            for (Map.Entry<String, Map<String, List<String>>> ent : map.entrySet()) {
                GenericTableEntityMgr tableEntityMgr = getTableEntityMgr(ent.getKey());
                Map<String, Object> result = tableEntityMgr.getByKeyPair(ent.getValue(), keyPair);
                if (MapUtils.isNotEmpty(result)) {
                    data.putAll(result);
                }
            }
        }
        return data;
    }

    private String lookupInternalAccountId(DynamoDataUnit lookupDataUnit, String lookupIdKey, String lookupIdValue) {
        if (lookupDataUnit != null) {
            String signature = lookupDataUnit.getSignature();
            GenericTableEntityMgr tableEntityMgr = getTableEntityMgr(signature);
            String tenantId = parseTenantId(lookupDataUnit);
            String tableName = parseTableName(lookupDataUnit);
            List<Pair<String, String>> keyPairs = new ArrayList<>();
            if (StringUtils.isNotBlank(lookupIdKey)) {
                keyPairs.add(Pair.of(lookupIdKey + "_" + lookupIdValue.toLowerCase(), "0"));
            }
            keyPairs.add(Pair.of(InterfaceName.AccountId.name() + "_" + lookupIdValue.toLowerCase(), "0"));
            List<Map<String, Object>> rows = tableEntityMgr.getByKeyPairs(tenantId, tableName, keyPairs);
            String accountId = null;

            for (Map<String, Object> row : rows) {
                if (row != null && StringUtils.isBlank(accountId)) {
                    accountId = row.getOrDefault(InterfaceName.AccountId.name(), "").toString();
                }
            }
            return accountId;
        } else {
            return null;
        }
    }

    private String getCacheKey(String tenant, TableRoleInCollection role, DataCollection.Version version) {
        return tenant + "|" + role.name() + (version != null ? "|" + version.name() : "");
    }

    private DynamoDataUnit getDynamoDataUnit(String cacheKey) {
        String[] tokens = cacheKey.split("\\|");
        String tenant = tokens[0];
        TableRoleInCollection role = TableRoleInCollection.valueOf(tokens[1]);
        DataCollection.Version version = tokens.length > 2 ? DataCollection.Version.valueOf(tokens[2]) : null;
        return dataCollectionProxy.getDynamoDataUnitByTableRole(tenant, version, role);
    }

    private List<DynamoDataUnit> getCustomDynamoDataUnits(String cacheKey) {
        String[] tokens = cacheKey.split("\\|");
        String tenant = tokens[0];
        DataCollection.Version version = tokens.length > 1 ? DataCollection.Version.valueOf(tokens[1]) : null;
        List<TableRoleInCollection> servingTables = new ArrayList<>();
        for (BusinessEntity entity : BusinessEntity.ACCOUNT_MATCH_ENTITIES) {
            TableRoleInCollection servingTable = BusinessEntity.Account.equals(entity) ? ConsolidatedAccount
                    : entity.getServingStore();
            servingTables.add(servingTable);
        }
        return dataCollectionProxy.getDynamoDataUnits(tenant, version, servingTables);
    }

    private String parseTenantId(DynamoDataUnit dynamoDataUnit) {
        return StringUtils.isNotBlank(dynamoDataUnit.getLinkedTenant()) ? dynamoDataUnit.getLinkedTenant()
                : dynamoDataUnit.getTenant();
    }

    private String parseTableName(DynamoDataUnit dynamoDataUnit) {
        return StringUtils.isNotEmpty(dynamoDataUnit.getLinkedTable()) ? dynamoDataUnit.getLinkedTable()
                : dynamoDataUnit.getName();
    }

    private Set<String> extractColumnNames(ColumnSelection columnSelection) {
        Set<String> columns = new HashSet<>();
        if (CollectionUtils.isNotEmpty(columnSelection.getColumns())) {
            columnSelection.getColumns().forEach(cm -> columns.add(cm.getExternalColumnId()));
        }
        return columns;
    }

    private GenericTableEntityMgr getTableEntityMgr(String signature) {
        if (!dynamoDataStoreMap.containsKey(signature)) {
            registerTableEntityMgr(signature);
        }
        return dynamoDataStoreMap.get(signature);
    }

    private synchronized void registerTableEntityMgr(String signature) {
        GenericTableEntityMgr tableEntityMgr = new GenericTableEntityMgrImpl(messageService, dataService, signature);
        dynamoDataStoreMap.putIfAbsent(signature, tableEntityMgr);
        log.info("Registered a GenericTableEntityMgr using signature " + signature);
    }

    @VisibleForTesting
    void setServingStoreProxy(ServingStoreProxy servingStoreProxy) {
        this.servingStoreProxy = servingStoreProxy;
    }

    @VisibleForTesting
    void setDataCollectionProxy(DataCollectionProxy dataCollectionProxy) {
        this.dataCollectionProxy = dataCollectionProxy;
    }

}
