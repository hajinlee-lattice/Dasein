package com.latticeengines.datacloud.match.service.impl;

import java.util.ArrayList;
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
import com.latticeengines.datacloud.match.service.CDLMatchService;
import com.latticeengines.datafabric.entitymanager.GenericTableEntityMgr;
import com.latticeengines.datafabric.entitymanager.impl.GenericTableEntityMgrImpl;
import com.latticeengines.datafabric.service.datastore.FabricDataService;
import com.latticeengines.datafabric.service.message.FabricMessageService;
import com.latticeengines.domain.exposed.datacloud.match.MatchInput;
import com.latticeengines.domain.exposed.metadata.ColumnMetadata;
import com.latticeengines.domain.exposed.metadata.DataCollection;
import com.latticeengines.domain.exposed.metadata.InterfaceName;
import com.latticeengines.domain.exposed.metadata.datastore.DynamoDataUnit;
import com.latticeengines.domain.exposed.propdata.manage.ColumnSelection;
import com.latticeengines.domain.exposed.query.BusinessEntity;
import com.latticeengines.domain.exposed.serviceapps.core.AttrState;
import com.latticeengines.proxy.exposed.cdl.DataCollectionProxy;
import com.latticeengines.proxy.exposed.cdl.ServingStoreProxy;

import reactor.core.publisher.Flux;

@Service("CDLMatchService")
public class CDLMatchServiceImpl implements CDLMatchService {

    private static final Logger log = LoggerFactory.getLogger(CDLMatchServiceImpl.class);

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
        DataCollection.Version version = input.getDataCollectionVersion();
        // TODO: get metadata by version
        List<ColumnMetadata> cms = servingStoreProxy.getDecoratedMetadataFromCache(customerSpace,
                BusinessEntity.Account);
        Flux<ColumnMetadata> flux = Flux.fromIterable(cms) //
                .filter(cm -> !AttrState.Inactive.equals(cm.getAttrState()));

        final List<ColumnSelection.Predefined> finalPredefinedList = new ArrayList<>(predefinedList);
        final Set<String> finalExtraColumns = new HashSet<>(extraColumns);
        flux = flux.filter(cm -> {
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
            return inExtraColumnList || inPredefinedList;
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
    public Map<String, Object> lookup(DynamoDataUnit dynamoDataUnit, String lookupIdKey, String lookupIdValue) {
        Map<String, Object> account = new HashMap<>();
        if (dynamoDataUnit != null) {
            GenericTableEntityMgr tableEntityMgr = getTableEntityMgr(dynamoDataUnit.getSignature());
            String tenantId = StringUtils.isNotBlank(dynamoDataUnit.getLinkedTenant())
                    ? dynamoDataUnit.getLinkedTenant() : dynamoDataUnit.getTenant();
            String tableName = StringUtils.isNotEmpty(dynamoDataUnit.getLinkedTable()) ? dynamoDataUnit.getLinkedTable()
                    : dynamoDataUnit.getName();
            if (InterfaceName.AccountId.name().equals(lookupIdKey)) {
                Pair<String, String> keyPair = Pair.of(lookupIdValue, "0");
                Map<String, Object> result = tableEntityMgr.getByKeyPair(tenantId, tableName, keyPair);
                if (MapUtils.isNotEmpty(result)) {
                    account.putAll(result);
                }
            } else {
                throw new UnsupportedOperationException("Only support lookup by AccountId.");
            }
        } else {
            log.info("No dynamo data unit found for custom account.");
        }
        return account;
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
