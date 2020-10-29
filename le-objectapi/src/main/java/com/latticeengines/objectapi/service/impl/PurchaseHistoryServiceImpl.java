package com.latticeengines.objectapi.service.impl;

import static com.latticeengines.query.factory.AthenaQueryProvider.ATHENA_USER;
import static com.latticeengines.query.factory.PrestoQueryProvider.PRESTO_USER;

import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import javax.inject.Inject;
import javax.sql.DataSource;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Component;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.latticeengines.common.exposed.validator.annotation.NotNull;
import com.latticeengines.db.exposed.util.MultiTenantContext;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.exception.LedpCode;
import com.latticeengines.domain.exposed.exception.LedpException;
import com.latticeengines.domain.exposed.metadata.ColumnMetadata;
import com.latticeengines.domain.exposed.metadata.DataCollection;
import com.latticeengines.domain.exposed.metadata.DataCollectionStatus;
import com.latticeengines.domain.exposed.metadata.InterfaceName;
import com.latticeengines.domain.exposed.metadata.transaction.ProductType;
import com.latticeengines.domain.exposed.query.BusinessEntity;
import com.latticeengines.domain.exposed.query.DataPage;
import com.latticeengines.domain.exposed.security.Tenant;
import com.latticeengines.domain.exposed.ulysses.PeriodTransaction;
import com.latticeengines.domain.exposed.ulysses.ProductHierarchy;
import com.latticeengines.objectapi.service.PurchaseHistoryService;
import com.latticeengines.prestodb.exposed.service.AthenaService;
import com.latticeengines.prestodb.exposed.service.PrestoConnectionService;
import com.latticeengines.proxy.exposed.cdl.DataCollectionProxy;
import com.latticeengines.proxy.exposed.cdl.ServingStoreProxy;
import com.latticeengines.redshiftdb.exposed.service.RedshiftPartitionService;

@Component("purchaseHistoryService")
public class PurchaseHistoryServiceImpl implements PurchaseHistoryService {

    private static final Logger log = LoggerFactory.getLogger(PurchaseHistoryServiceImpl.class);

    @Inject
    private DataCollectionProxy dataCollectionProxy;

    @Inject
    private ServingStoreProxy servingStoreProxy;

    @Inject
    private RedshiftPartitionService redshiftPartitionService;

    @Inject
    private PrestoConnectionService prestoConnectionService;

    @Inject
    private AthenaService athenaService;

    @SuppressWarnings("rawtypes")
    @Override
    public List<PeriodTransaction> getPeriodTransactionsByAccountId(String accountId, String periodName,
            ProductType productType, String sqlUser) {
        List<PeriodTransaction> resultList = new ArrayList<>();
        Tenant tenant = MultiTenantContext.getTenant();
        String periodTransactionTableName = getAndValidateServingStoreTableName(tenant.getId(),
                BusinessEntity.PeriodTransaction);
        if (log.isDebugEnabled()) {
            log.debug(String.format(
                    "Get Period Transaction table %s for %s with account %s and periodName %s, productType %s",
                    periodTransactionTableName, tenant.getId(), accountId, periodName, productType));
        }
        // For BIS use case, the product type is ProductType.Spending
        String baseQuery = "SELECT t.{0}, t.{1}, t.{2}, t.{3}, t.{4} FROM {5} t "
                + "where t.{6} = ? and t.{7} = ''{10}'' and t.{8} = ''{9}''";
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
                productType.toString(), // 9
                periodName // 10
        );
        if (log.isDebugEnabled()) {
            log.debug("Query for getPeriodTransactionsByAccountId " + query);
        }
        List<Map<String, Object>> retList;
        if (ATHENA_USER.equalsIgnoreCase(sqlUser)) {
            // athena only support plain query
            query = query.replace("?", String.format("'%s'", accountId));
            retList = query(query, sqlUser);
        } else {
            retList = query(query, sqlUser, accountId);
        }
        for (Map row : retList) {
            PeriodTransaction periodTransaction = new PeriodTransaction();
            periodTransaction.setTotalAmount((double) row.get(InterfaceName.TotalAmount.toString().toLowerCase()));
            periodTransaction.setTotalQuantity((double) row.get(InterfaceName.TotalQuantity.toString().toLowerCase()));
            periodTransaction
                    .setTransactionCount(castToDouble(row.get(InterfaceName.TransactionCount.toString().toLowerCase())));
            periodTransaction.setProductId((String) row.get(InterfaceName.ProductId.toString().toLowerCase()));
            periodTransaction.setPeriodId((int) row.get(InterfaceName.PeriodId.toString().toLowerCase()));
            resultList.add(periodTransaction);
        }
        return resultList;
    }

    @SuppressWarnings("rawtypes")
    @Override
    public List<PeriodTransaction> getPeriodTransactionsForSegmentAccounts(String segment, String periodName,
            ProductType productType, String sqlUser) {
        List<PeriodTransaction> resultList = new ArrayList<>();
        Tenant tenant = MultiTenantContext.getTenant();
        String periodTransactionTableName = getAndValidateServingStoreTableName(tenant.getId(),
                BusinessEntity.PeriodTransaction);
        String accountTableName = getAndValidateServingStoreTableName(tenant.getId(), BusinessEntity.Account);
        if (log.isDebugEnabled()) {
            log.debug(String.format(
                    "Get Period Transaction Table %s, Account Table %s for %s with segment %s and periodName %s, productType %s",
                    periodTransactionTableName, accountTableName, tenant.getId(), segment, periodName, productType));
        }

        String baseQuery = "SELECT " //
                + "count(pt.{0}), " //
                + "pt.{1}, " //
                + "pt.{2}, " //
                + "sum(pt.{3}) as {11}, " //
                + "sum(pt.{4}) as {12}, " //
                + "sum(pt.{5}) as {13} " //
                + "FROM {6} as pt " //
                + "inner join {7} as ac on  pt.{0} = ac.{0} " //
                + "where pt.{8} = ''{14}'' and ac.{9} = ''{15}'' and pt.{10} = ''{16}'' " //
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
                InterfaceName.TransactionCount, // 13
                periodName, // 14
                segment, // 15
                productType.name() // 16
        );
        if (log.isDebugEnabled()) {
            log.debug("Query for getPeriodTransactionForSegmentAccount " + query);
        }
        List<Map<String, Object>> retList = query(query, sqlUser);
        for (Map row : retList) {
            PeriodTransaction periodTransaction = new PeriodTransaction();
            periodTransaction.setTotalAmount((double) row.get(InterfaceName.TotalAmount.toString().toLowerCase()));
            periodTransaction.setTotalQuantity((double) row.get(InterfaceName.TotalQuantity.toString().toLowerCase()));
            periodTransaction
                    .setTransactionCount(castToDouble(row.get(InterfaceName.TransactionCount.toString().toLowerCase())));
            periodTransaction.setProductId((String) row.get(InterfaceName.ProductId.toString().toLowerCase()));
            periodTransaction.setPeriodId((int) row.get(InterfaceName.PeriodId.toString().toLowerCase()));
            resultList.add(periodTransaction);
        }
        return resultList;
    }

    private double castToDouble(Object obj) {
        Preconditions.checkNotNull(obj);
        return Double.parseDouble(obj.toString());
    }

    @SuppressWarnings("rawtypes")
    @Override
    public List<ProductHierarchy> getProductHierarchy(DataCollection.Version version, String sqlUser) {
        List<ProductHierarchy> resultList = new ArrayList<>();
        Tenant tenant = MultiTenantContext.getTenant();
        String tableName = getAndValidateServingStoreTableName(tenant.getId(), BusinessEntity.ProductHierarchy);
        if (log.isDebugEnabled()) {
            log.debug(String.format("Get product Hierarchy table %s for %s", tableName, tenant.getId()));
        }
        String query = String.format(
                "SELECT %s, %s, %s, coalesce(" + InterfaceName.ProductLineId + ", " + InterfaceName.ProductFamilyId
                        + ", " + InterfaceName.ProductCategoryId + ") as productid FROM %s",
                InterfaceName.ProductLine, InterfaceName.ProductFamily, InterfaceName.ProductCategory, tableName);
        if (log.isDebugEnabled()) {
            log.debug("query for getProductHierarchy " + query);
        }
        List<Map<String, Object>> retList = query(query, sqlUser);
        for (Map row : retList) {
            ProductHierarchy productHierarchy = new ProductHierarchy();
            productHierarchy
                    .setProductCategory((String) row.get(InterfaceName.ProductCategory.toString().toLowerCase()));
            productHierarchy.setProductFamily((String) row.get(InterfaceName.ProductFamily.toString().toLowerCase()));
            productHierarchy.setProductLine((String) row.get(InterfaceName.ProductLine.toString().toLowerCase()));
            productHierarchy.setProductId((String) row.get("productid"));
            resultList.add(productHierarchy);
        }
        return resultList;
    }

    @Override
    public DataPage getAllSpendAnalyticsSegments(String sqlUser) {
        Tenant tenant = MultiTenantContext.getTenant();
        // SpendAnalyticsSegment is optional in Account data. Check to see if
        // this column exists. If not, return empty result
        List<ColumnMetadata> acctCMList = servingStoreProxy.getDecoratedMetadataFromCache(
                MultiTenantContext.getCustomerSpace().toString(), BusinessEntity.Account);

        if (acctCMList.stream()
                .anyMatch(cm -> cm.getAttrName().equalsIgnoreCase(InterfaceName.SpendAnalyticsSegment.name()))) {
            try {
                String periodTransactionTableName = getAndValidateServingStoreTableName(tenant.getId(),
                        BusinessEntity.PeriodTransaction);
                String accountTableName = getAndValidateServingStoreTableName(tenant.getId(), BusinessEntity.Account);
                if (log.isDebugEnabled()) {
                    log.debug(String.format("Get Period Transaction Table %s, Account Table %s for %s ",
                            periodTransactionTableName, accountTableName, tenant.getId()));
                }
                String query = MessageFormat.format(
                        "SELECT distinct ac.{0}, count(ac.{1}) as {2}, True as IsSegment, ac.{0} as AccountId " //
                                + "FROM {3} as ac " //
                                + "WHERE ac.{0} IS NOT NULL and ac.{1} in " //
                                + "(select distinct pt.{1} from {4} as pt where pt.{5} = ''{6}'') " //
                                + "GROUP BY ac.{0} " //
                                + "ORDER BY ac.{0}", //
                        InterfaceName.SpendAnalyticsSegment, // 0
                        InterfaceName.AccountId, // 1
                        InterfaceName.RepresentativeAccounts, // 2
                        accountTableName, // 3
                        periodTransactionTableName, // 4
                        InterfaceName.ProductType, // 5
                        ProductType.Spending.name() // 6
                );
                if (log.isDebugEnabled()) {
                    log.debug("query for getAllSpendAnalyticsSegments " + query);
                }
                List<Map<String, Object>> retList = query(query, sqlUser);
                return new DataPage(retList);
            } catch (Exception e) {
                if (ExceptionUtils.getStackTrace(e).contains("spendanalyticssegment does not exist")) {
                    log.warn("spendanalyticssegment column does not exist");
                    return new DataPage(Collections.emptyList());
                } else {
                    throw e;
                }
            }
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
    public List<String> getFinalAndFirstTransactionDate(String sqlUser) {
        Tenant tenant = MultiTenantContext.getTenant();
        String transactionTableName = getAndValidateServingStoreTableName(tenant.getId(), BusinessEntity.Transaction);
        if (log.isDebugEnabled()) {
            log.debug(String.format("Get Transaction Table %s for %s", transactionTableName, tenant.getId()));
        }
        String query = MessageFormat.format("SELECT max ({0}) as max, min ({0}) as min FROM {1}",
                InterfaceName.TransactionDate, transactionTableName);
        if (log.isDebugEnabled()) {
            log.debug("query for getFinalTransactionDate " + query);
        }
        List<Map<String, Object>> retList = query(query, sqlUser);
        return Arrays.asList((String) retList.get(0).get("max"), (String) retList.get(0).get("min"));
    }

    private List<Map<String, Object>> query(String sql, String sqlUser, Object... params) {
        List<Map<String, Object>> retList;
        switch (sqlUser) {
            case ATHENA_USER:
                retList = athenaService.query(sql);
                break;
            case PRESTO_USER:
                DataSource dataSource = prestoConnectionService.getPrestoDataSource();
                JdbcTemplate jdbcTemplate1 = new JdbcTemplate(dataSource);
                retList = jdbcTemplate1.queryForList(sql, params);
                break;
            default:
                CustomerSpace customerSpace = MultiTenantContext.getCustomerSpace();
                JdbcTemplate jdbcTemplate2 = getRedshiftJdbcTemplate(customerSpace, null);
                retList = jdbcTemplate2.queryForList(sql, params);
        }
        return retList;
    }

    /*-
     * Get redshift jdbc template for target tenant in specific version (null to indicate current active version)
     */
    private JdbcTemplate getRedshiftJdbcTemplate(@NotNull CustomerSpace customerSpace, DataCollection.Version version) {
        Preconditions.checkNotNull(customerSpace, "customer space object should not be null");
        DataCollectionStatus status = dataCollectionProxy.getOrCreateDataCollectionStatus(customerSpace.toString(),
                version);
        String partition = StringUtils.defaultIfBlank(status.getRedshiftPartition(), null);
        return redshiftPartitionService.getSegmentUserJdbcTemplate(partition);
    }

}
