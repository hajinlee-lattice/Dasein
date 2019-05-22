package com.latticeengines.objectapi.service.impl;

import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import javax.annotation.Resource;
import javax.inject.Inject;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Component;
import org.springframework.util.StringUtils;

import com.google.common.annotations.VisibleForTesting;
import com.latticeengines.db.exposed.util.MultiTenantContext;
import com.latticeengines.domain.exposed.exception.LedpCode;
import com.latticeengines.domain.exposed.exception.LedpException;
import com.latticeengines.domain.exposed.metadata.ColumnMetadata;
import com.latticeengines.domain.exposed.metadata.DataCollection;
import com.latticeengines.domain.exposed.metadata.InterfaceName;
import com.latticeengines.domain.exposed.metadata.transaction.ProductType;
import com.latticeengines.domain.exposed.query.BusinessEntity;
import com.latticeengines.domain.exposed.query.DataPage;
import com.latticeengines.domain.exposed.security.Tenant;
import com.latticeengines.domain.exposed.ulysses.PeriodTransaction;
import com.latticeengines.domain.exposed.ulysses.ProductHierarchy;
import com.latticeengines.objectapi.service.PurchaseHistoryService;
import com.latticeengines.proxy.exposed.cdl.DataCollectionProxy;
import com.latticeengines.proxy.exposed.cdl.ServingStoreProxy;

@Component("purchaseHistoryService")
public class PurchaseHistoryServiceImpl implements PurchaseHistoryService {

    private static final Logger log = LoggerFactory.getLogger(PurchaseHistoryServiceImpl.class);

    @Resource(name = "redshiftJdbcTemplate")
    private JdbcTemplate redshiftJdbcTemplate;

    @Inject
    private DataCollectionProxy dataCollectionProxy;

    @Inject
    private ServingStoreProxy servingStoreProxy;

    @SuppressWarnings("rawtypes")
    @Override
    public List<PeriodTransaction> getPeriodTransactionsByAccountId(String accountId, String periodName,
            ProductType productType) {
        List<PeriodTransaction> resultList = new ArrayList<>();
        Tenant tenant = MultiTenantContext.getTenant();
        String periodTransactionTableName = getAndValidateServingStoreTableName(tenant.getId(),
                BusinessEntity.PeriodTransaction);

        log.info(String.format(
                "Get Period Transaction table %s for %s with account %s and periodName %s, productType %s",
                periodTransactionTableName, tenant.getId(), accountId, periodName, productType));

        // For BIS use case, the product type is ProductType.Spending
        String baseQuery = "SELECT t.{0}, t.{1}, t.{2}, t.{3}, t.{4} FROM {5} t "
                + "where t.{6} = ? and t.{7} = ? and t.{8} = ''{9}''";
        String query = MessageFormat.format(baseQuery, //
                InterfaceName.PeriodId, // 0
                InterfaceName.ProductId, // 1
                InterfaceName.TotalAmount, // 2
                InterfaceName.TotalQuantity, // 3
                InterfaceName.TransactionCount, // 4
                periodTransactionTableName, // 5
                InterfaceName.AccountId, // 6
                InterfaceName.PeriodName, // 7
                InterfaceName.ProductType, // 8
                productType.toString()); // 9

        log.info("Query for getPeriodTransactionsByAccountId " + query);
        List<Map<String, Object>> retList = redshiftJdbcTemplate.queryForList(query, accountId, periodName);
        for (Map row : retList) {
            PeriodTransaction periodTransaction = new PeriodTransaction();
            periodTransaction.setTotalAmount((double) row.get(InterfaceName.TotalAmount.toString().toLowerCase()));
            periodTransaction.setTotalQuantity((double) row.get(InterfaceName.TotalQuantity.toString().toLowerCase()));
            periodTransaction
                    .setTransactionCount((double) row.get(InterfaceName.TransactionCount.toString().toLowerCase()));
            periodTransaction.setProductId((String) row.get(InterfaceName.ProductId.toString().toLowerCase()));
            periodTransaction.setPeriodId((int) row.get(InterfaceName.PeriodId.toString().toLowerCase()));
            resultList.add(periodTransaction);
        }
        log.info("resultList for account is " + resultList);
        return resultList;
    }

    @SuppressWarnings("rawtypes")
    @Override
    public List<PeriodTransaction> getPeriodTransactionsForSegmentAccounts(String segment, String periodName,
            ProductType productType) {
        List<PeriodTransaction> resultList = new ArrayList<>();
        Tenant tenant = MultiTenantContext.getTenant();
        String periodTransactionTableName = getAndValidateServingStoreTableName(tenant.getId(),
                BusinessEntity.PeriodTransaction);
        String accountTableName = getAndValidateServingStoreTableName(tenant.getId(), BusinessEntity.Account);

        log.info(String.format(
                "Get Period Transaction Table %s, Account Table %s for %s with segment %s and periodName %s, productType %s",
                periodTransactionTableName, accountTableName, tenant.getId(), segment, periodName, productType));
        String baseQuery = "SELECT count(pt.{0}), pt.{1}, pt.{2}, sum(pt.{3}) as {11}, sum(pt.{4}) as {12}, sum(pt.{5}) as {13} FROM {6} as pt "
                + "inner join {7} as ac on  pt.{0} = ac.{0} " //
                + "where pt.{8} = ? and ac.{9} = ? and pt.{10} = ? " //
                + "group by pt.{1}, pt.{2} " //
                + "order by pt.{1}, pt.{2}";

        String query = MessageFormat.format(baseQuery, //
                InterfaceName.AccountId, // 0
                InterfaceName.PeriodId, // 1
                InterfaceName.ProductId, // 2
                InterfaceName.TotalAmount, // 3
                InterfaceName.TotalQuantity, // 4
                InterfaceName.TransactionCount, // 5
                periodTransactionTableName, // 6
                accountTableName, // 7
                InterfaceName.PeriodName, // 8
                InterfaceName.SpendAnalyticsSegment, // 9
                InterfaceName.ProductType, // 10
                InterfaceName.TotalAmount, // 11
                InterfaceName.TotalQuantity, // 12
                InterfaceName.TransactionCount); // 13

        log.info("Query for getPeriodTransactionForSegmentAccount " + query);
        List<Map<String, Object>> retList = redshiftJdbcTemplate.queryForList(query, periodName, segment,
                productType.toString());
        for (Map row : retList) {
            PeriodTransaction periodTransaction = new PeriodTransaction();
            periodTransaction.setTotalAmount((double) row.get(InterfaceName.TotalAmount.toString().toLowerCase()));
            periodTransaction.setTotalQuantity((double) row.get(InterfaceName.TotalQuantity.toString().toLowerCase()));
            periodTransaction
                    .setTransactionCount((double) row.get(InterfaceName.TransactionCount.toString().toLowerCase()));
            periodTransaction.setProductId((String) row.get(InterfaceName.ProductId.toString().toLowerCase()));
            periodTransaction.setPeriodId((int) row.get(InterfaceName.PeriodId.toString().toLowerCase()));
            resultList.add(periodTransaction);
        }
        log.info("resultList for segment is " + resultList);
        return resultList;
    }

    @SuppressWarnings("rawtypes")
    @Override
    public List<ProductHierarchy> getProductHierarchy(DataCollection.Version version) {
        List<ProductHierarchy> resultList = new ArrayList<>();
        Tenant tenant = MultiTenantContext.getTenant();
        String tableName = getAndValidateServingStoreTableName(tenant.getId(), BusinessEntity.ProductHierarchy);
        log.info(String.format("Get product Hierarchy table %s for %s", tableName, tenant.getId()));
        String query = String.format(
                "SELECT %s, %s, %s, coalesce(" + InterfaceName.ProductLineId + ", " + InterfaceName.ProductFamilyId
                        + ", " + InterfaceName.ProductCategoryId + ") as productid FROM %s",
                InterfaceName.ProductLine, InterfaceName.ProductFamily, InterfaceName.ProductCategory, tableName);
        log.info("query for getProductHierarchy " + query);
        List<Map<String, Object>> retList = redshiftJdbcTemplate.queryForList(query);
        for (Map row : retList) {
            ProductHierarchy productHierarchy = new ProductHierarchy();
            productHierarchy
                    .setProductCategory((String) row.get(InterfaceName.ProductCategory.toString().toLowerCase()));
            productHierarchy.setProductFamily((String) row.get(InterfaceName.ProductFamily.toString().toLowerCase()));
            productHierarchy.setProductLine((String) row.get(InterfaceName.ProductLine.toString().toLowerCase()));
            productHierarchy.setProductId((String) row.get("productid"));
            resultList.add(productHierarchy);
        }
        log.info("resultList is " + resultList);
        return resultList;
    }

    @Override
    public DataPage getAllSpendAnalyticsSegments() {
        Tenant tenant = MultiTenantContext.getTenant();
        // SpendAnalyticsSegment is optional in Account data. Check to see if
        // this column exists. If not, return empty result
        List<ColumnMetadata> acctCMList = servingStoreProxy.getDecoratedMetadataFromCache(
                MultiTenantContext.getCustomerSpace().toString(), BusinessEntity.Account);

        if (acctCMList.stream()
                .anyMatch(cm -> cm.getAttrName().equalsIgnoreCase(InterfaceName.SpendAnalyticsSegment.name()))) {
            String periodTransactionTableName = getAndValidateServingStoreTableName(tenant.getId(),
                    BusinessEntity.PeriodTransaction);
            String accountTableName = getAndValidateServingStoreTableName(tenant.getId(), BusinessEntity.Account);
            log.info(String.format("Get Period Transaction Table %s, Account Table %s for %s ",
                    periodTransactionTableName, accountTableName, tenant.getId()));

            String query = MessageFormat.format(
                    "SELECT distinct ac.{0}, count(ac.{1}) as {2}, True as IsSegment, ac.{0} as AccountId " //
                            + "FROM {3} as ac " //
                            + "WHERE ac.{0} IS NOT NULL and ac.{1} in " //
                            + "(select distinct pt.{1} from {4} as pt where pt.{5} = ?) " //
                            + "GROUP BY ac.{0} " //
                            + "ORDER BY ac.{0}", //
                    InterfaceName.SpendAnalyticsSegment, // 0
                    InterfaceName.AccountId, // 1
                    InterfaceName.RepresentativeAccounts, // 2
                    accountTableName, // 3
                    periodTransactionTableName, // 4
                    InterfaceName.ProductType); // 5
            log.info("query for getAllSpendAnalyticsSegments " + query);
            List<Map<String, Object>> retList = redshiftJdbcTemplate.queryForList(query, ProductType.Spending.name());
            return new DataPage(retList);
        }
        return new DataPage(Collections.emptyList());
    }

    private String getAndValidateServingStoreTableName(String customerSpace, BusinessEntity businessEntity) {
        String toReturn = dataCollectionProxy.getTableName(customerSpace, businessEntity.getServingStore());
        if (StringUtils.isEmpty(toReturn)) {
            throw new LedpException(LedpCode.LEDP_37017,
                    new String[] { "ServingStore" + businessEntity.name(), customerSpace });
        }
        return toReturn;
    }

    @VisibleForTesting
    void setDataCollectionProxy(DataCollectionProxy dataCollectionProxy) {
        this.dataCollectionProxy = dataCollectionProxy;
    }

    @Override
    public List<String> getFinalAndFirstTransactionDate() {
        Tenant tenant = MultiTenantContext.getTenant();
        String transactionTableName = getAndValidateServingStoreTableName(tenant.getId(), BusinessEntity.Transaction);
        log.info(String.format("Get Transaction Table %s for %s", transactionTableName, tenant.getId()));
        String query = MessageFormat.format("SELECT max ({0}) as max, min ({0}) as min FROM {1}",
                InterfaceName.TransactionDate, transactionTableName);
        log.info("query for getFinalTransactionDate " + query);
        List<Map<String, Object>> retList = redshiftJdbcTemplate.queryForList(query);
        return Arrays.asList((String) retList.get(0).get("max"), (String) retList.get(0).get("min"));
    }

}
