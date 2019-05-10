package com.latticeengines.datacloud.match.service.impl;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import javax.inject.Inject;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.collections4.MapUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import com.google.common.annotations.VisibleForTesting;
import com.latticeengines.datacloud.match.service.CDLLookupService;
import com.latticeengines.datafabric.entitymanager.GenericTableEntityMgr;
import com.latticeengines.datafabric.entitymanager.impl.GenericTableEntityMgrImpl;
import com.latticeengines.datafabric.service.datastore.FabricDataService;
import com.latticeengines.datafabric.service.message.FabricMessageService;
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

    @Override
    public List<ColumnMetadata> parseMetadata(MatchInput input) {
        List<ColumnSelection.Predefined> predefinedList = Collections.emptyList();
        Set<String> extraColumns = Collections.emptySet();
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
        }

        String customerSpace = input.getTenant().getId();
        @SuppressWarnings("unused")
        DataCollection.Version version = input.getDataCollectionVersion();
        // TODO: get metadata by version
        BusinessEntity[] entities = { //
                BusinessEntity.Account, //
                BusinessEntity.CuratedAccount, //
                BusinessEntity.Rating, //
                BusinessEntity.PurchaseHistory //
        };
        List<ColumnMetadata> cms = new ArrayList<>();
        for (BusinessEntity entity : entities) {
            List<ColumnMetadata> list = servingStoreProxy.getDecoratedMetadataFromCache(customerSpace, entity);
            if (CollectionUtils.isNotEmpty(list)) {
                cms.addAll(list);
            }
        }
        Flux<ColumnMetadata> flux = Flux.fromIterable(cms) //
                .filter(cm -> !AttrState.Inactive.equals(cm.getAttrState()));

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
    public DynamoDataUnit parseCustomAccountDynamo(MatchInput input) {
        String customerSpace = input.getTenant().getId();
        DataCollection.Version version = input.getDataCollectionVersion();
        return dataCollectionProxy.getAccountDynamo(customerSpace, version);
    }

    @Override
    public List<DynamoDataUnit> parseCustomDynamo(MatchInput input) {
        String customerSpace = input.getTenant().getId();
        DataCollection.Version version = input.getDataCollectionVersion();
        TableRoleInCollection[] tableRoles = { //
                TableRoleInCollection.ConsolidatedAccount, //
                TableRoleInCollection.PivotedRating, //
                TableRoleInCollection.CalculatedPurchaseHistory, //
                TableRoleInCollection.CalculatedCuratedAccountAttribute //
        };
        return dataCollectionProxy.getDynamoDataUnits(customerSpace, version, Arrays.asList(tableRoles));
    }

    @Override
    public Map<String, Object> lookup(List<DynamoDataUnit> dynamoDataUnits, String lookupIdKey, String lookupIdValue) {
        if (!InterfaceName.AccountId.name().equals(lookupIdKey)) {
            throw new UnsupportedOperationException("Only support lookup by AccountId.");
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
                String tenantId = StringUtils.isNotBlank(dynamoDataUnit.getLinkedTenant())
                        ? dynamoDataUnit.getLinkedTenant()
                        : dynamoDataUnit.getTenant();
                if (!map.get(dynamoDataUnit.getSignature()).containsKey(tenantId)) {
                    map.get(dynamoDataUnit.getSignature()).put(tenantId, new ArrayList<>());
                }
                String tableName = StringUtils.isNotEmpty(dynamoDataUnit.getLinkedTable())
                        ? dynamoDataUnit.getLinkedTable()
                        : dynamoDataUnit.getName();
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

    private Set<String> extractColumnNames(ColumnSelection columnSelection) {
        Set<String> columns = new HashSet<>();
        if (CollectionUtils.isNotEmpty(columnSelection.getColumns())) {
            columnSelection.getColumns().forEach(cm -> {
                columns.add(cm.getExternalColumnId());
            });
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
