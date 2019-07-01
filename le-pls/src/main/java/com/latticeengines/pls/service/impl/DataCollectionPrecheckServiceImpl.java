package com.latticeengines.pls.service.impl;

import java.util.List;

import javax.inject.Inject;

import org.apache.commons.collections4.CollectionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.cdl.PeriodStrategy;
import com.latticeengines.domain.exposed.metadata.ColumnMetadata;
import com.latticeengines.domain.exposed.metadata.DataCollection;
import com.latticeengines.domain.exposed.metadata.InterfaceName;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.metadata.TableRoleInCollection;
import com.latticeengines.domain.exposed.query.BusinessEntity;
import com.latticeengines.domain.exposed.serviceapps.cdl.DataCollectionPrechecks;
import com.latticeengines.pls.service.DataCollectionPrecheckService;
import com.latticeengines.proxy.exposed.cdl.DataCollectionProxy;
import com.latticeengines.proxy.exposed.cdl.DataFeedProxy;
import com.latticeengines.proxy.exposed.cdl.ServingStoreProxy;

@Component("dataCollectionPrecheckService")
public class DataCollectionPrecheckServiceImpl implements DataCollectionPrecheckService {
    private static final Logger log = LoggerFactory.getLogger(DataCollectionPrecheckServiceImpl.class);

    private final ServingStoreProxy servingStoreProxy;
    private final DataCollectionProxy dataCollectionProxy;
    private final DataFeedProxy dataFeedProxy;

    @Inject
    public DataCollectionPrecheckServiceImpl(ServingStoreProxy servingStoreProxy,
                                             DataCollectionProxy dataCollectionProxy, DataFeedProxy dataFeedProxy) {
        this.servingStoreProxy = servingStoreProxy;
        this.dataCollectionProxy = dataCollectionProxy;
        this.dataFeedProxy = dataFeedProxy;
    }

    @Override
    public DataCollectionPrechecks validateDataCollectionPrechecks(String customerSpace) {
        DataCollectionPrechecks prechecks = new DataCollectionPrechecks();
        prechecks.setDisableAllCuratedMetrics(true);
        prechecks.setDisableShareOfWallet(true);
        prechecks.setDisableMargin(true);
        prechecks.setDisableCrossSellModeling(true);

        List<ColumnMetadata> prodCMList =
                servingStoreProxy.getDecoratedMetadataFromCache(customerSpace, BusinessEntity.Product);
        List<ColumnMetadata> trxCMList =
                servingStoreProxy.getDecoratedMetadataFromCache(customerSpace, BusinessEntity.PeriodTransaction);
        List<ColumnMetadata> acctCMList =
                servingStoreProxy.getDecoratedMetadataFromCache(customerSpace, BusinessEntity.Account);

        DataCollection.Version version = dataCollectionProxy.getActiveVersion(customerSpace);
        Table acctTable = dataCollectionProxy.getTable(
                customerSpace, TableRoleInCollection.ConsolidatedAccount, version);
        Table prodTable = dataCollectionProxy.getTable(
                customerSpace, TableRoleInCollection.ConsolidatedProduct, version);
        List<Table> trxTables = dataCollectionProxy.getTables(
                customerSpace, TableRoleInCollection.ConsolidatedPeriodTransaction, version);
        Table apsTable = dataCollectionProxy.getTable(
                customerSpace, TableRoleInCollection.AnalyticPurchaseState, version);
        List<Table> transactionTemplateTables = dataFeedProxy.getTemplateTables(customerSpace,
                BusinessEntity.Transaction.name());

        boolean metadataReady = false;
        boolean tablesReady = false;

        if (CollectionUtils.isNotEmpty(prodCMList)
                && CollectionUtils.isNotEmpty(trxCMList)
                && CollectionUtils.isNotEmpty(acctCMList)) {
            metadataReady = true;
        }

        if (acctTable != null && prodTable != null && apsTable != null
                && CollectionUtils.isNotEmpty(trxTables)
                && trxTables.size() == PeriodStrategy.NATURAL_PERIODS.size()) {
            tablesReady = true;
        }

        if (metadataReady && tablesReady) {
            prechecks.setDisableCrossSellModeling(false);
        }

        if (metadataReady) {
            prechecks.setDisableAllCuratedMetrics(false);
        } else {
            return prechecks;
        }

        for (Table clpTable : transactionTemplateTables) {
            for (String attribute : clpTable.getAttributeNames()) {
                log.info(JsonUtils.serialize(clpTable));
                if (attribute.equalsIgnoreCase(InterfaceName.Cost.name())) {
                    prechecks.setDisableMargin(false);
                    break;
                }
            }
        }

        for (ColumnMetadata metadata : acctCMList) {
            if (metadata.getAttrName().equalsIgnoreCase(InterfaceName.SpendAnalyticsSegment.name())) {
                prechecks.setDisableShareOfWallet(false);
                break;
            }
        }

        return prechecks;
    }
}
