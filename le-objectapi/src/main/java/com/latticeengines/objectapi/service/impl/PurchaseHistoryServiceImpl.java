package com.latticeengines.objectapi.service.impl;

import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import javax.annotation.Resource;
import javax.inject.Inject;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Component;

import com.latticeengines.db.exposed.util.MultiTenantContext;
import com.latticeengines.domain.exposed.metadata.DataCollection;
import com.latticeengines.domain.exposed.metadata.InterfaceName;
import com.latticeengines.domain.exposed.metadata.transaction.ProductType;
import com.latticeengines.domain.exposed.query.BusinessEntity;
import com.latticeengines.domain.exposed.security.Tenant;
import com.latticeengines.domain.exposed.ulysses.PeriodTransaction;
import com.latticeengines.domain.exposed.ulysses.ProductHierarchy;
import com.latticeengines.objectapi.service.PurchaseHistoryService;
import com.latticeengines.proxy.exposed.cdl.DataCollectionProxy;

@Component("purchaseHistoryService")
public class PurchaseHistoryServiceImpl implements PurchaseHistoryService {

    private static final Logger log = LoggerFactory.getLogger(PurchaseHistoryServiceImpl.class);

    @Resource(name = "redshiftJdbcTemplate")
    private JdbcTemplate redshiftJdbcTemplate;

    @Inject
    private DataCollectionProxy dataCollectionProxy;

    @SuppressWarnings("rawtypes")
    @Override
    public List<PeriodTransaction> getPeriodTransactionByAccountId(String accountId, String periodName,
            DataCollection.Version version, ProductType productType) {
        List<PeriodTransaction> resultList = new ArrayList<>();
        Tenant tenant = MultiTenantContext.getTenant();
        String periodTransactionTableName = dataCollectionProxy.getTableName(tenant.getId(),
                BusinessEntity.PeriodTransaction.getServingStore());

        log.info(String.format(
                "Get Period Transaction table %s for %s with account %s and periodName %s, productType %s, version %s",
                periodTransactionTableName, tenant.getId(), accountId, periodName, productType, version));

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

        log.info("Query for getPeriodTransactionByAccountId " + query);
        List<Map<String, Object>> retList = redshiftJdbcTemplate.queryForList(query, accountId, periodName);
        for (Map row : retList) {
            PeriodTransaction periodTransaction = new PeriodTransaction();
            periodTransaction.setTotalAmount((Double) row.get(InterfaceName.TotalAmount.toString().toLowerCase()));
            periodTransaction.setTotalQuantity((Long) row.get(InterfaceName.TotalQuantity.toString().toLowerCase()));
            periodTransaction
                    .setTransactionCount((Double) row.get(InterfaceName.TransactionCount.toString().toLowerCase()));
            periodTransaction.setProductId((String) row.get(InterfaceName.ProductId.toString().toLowerCase()));
            periodTransaction.setPeriodId((Integer) row.get(InterfaceName.PeriodId.toString().toLowerCase()));
            resultList.add(periodTransaction);
        }
        log.info("resultList for account is " + resultList);
        return resultList;
    }

    @SuppressWarnings("rawtypes")
    @Override
    public List<PeriodTransaction> getPeriodTransactionForSegmentAccount(String segment, String periodName,
            ProductType productType) {
        List<PeriodTransaction> resultList = new ArrayList<>();
        Tenant tenant = MultiTenantContext.getTenant();
        String periodTransactionTableName = dataCollectionProxy.getTableName(tenant.getId(),
                BusinessEntity.PeriodTransaction.getServingStore());
        String accountTableName = dataCollectionProxy.getTableName(tenant.getId(),
                BusinessEntity.Account.getServingStore());

        log.info(String.format(
                "Get Period Transaction Table %s, Account Table %s for %s with segment %s and periodName %s, productType %s",
                periodTransactionTableName, accountTableName, tenant.getId(), segment, periodName, productType));
        String baseQuery = "SELECT count(pt.{0}), pt.{1}, pt.{2}, sum(pt.{3}) as totalaccount, sum(pt.{4}) as totalquantity, sum(pt.{5}) as transactioncount FROM {6} as pt "
                + "inner join {7} as ac on  pt.{0} = ac.{0} " //
                + "where pt.{8} = ? and ac.{9} = ? and pt.{10} = ?" //
                + "group by pt.{1}, pt.{2}" //
                + "sort by pt.{1}, pt.{2}";

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
                InterfaceName.ProductType); // 10

        log.info("Query for getPeriodTransactionForSegmentAccount " + query);
        List<Map<String, Object>> retList = redshiftJdbcTemplate.queryForList(query, periodName, segment,
                productType.toString());
        for (Map row : retList) {
            PeriodTransaction periodTransaction = new PeriodTransaction();
            periodTransaction.setTotalAmount((Double) row.get(InterfaceName.TotalAmount.toString().toLowerCase()));
            periodTransaction.setTotalQuantity((Long) row.get(InterfaceName.TotalQuantity.toString().toLowerCase()));
            periodTransaction
                    .setTransactionCount((Double) row.get(InterfaceName.TransactionCount.toString().toLowerCase()));
            periodTransaction.setProductId((String) row.get(InterfaceName.ProductId.toString().toLowerCase()));
            periodTransaction.setPeriodId((Integer) row.get(InterfaceName.PeriodId.toString().toLowerCase()));
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
        String tableName = dataCollectionProxy.getTableName(tenant.getId(),
                BusinessEntity.ProductHierarchy.getServingStore());
        log.info(String.format("Get product Hierarchy table %s for %s", tableName, tenant.getId()));
        String query = String.format(
                "SELECT %s, %s, %s, coalesce(" + InterfaceName.ProductLineId + ", " + InterfaceName.ProductFamilyId
                        + ", " + InterfaceName.ProductCategoryId + ") as productid FROM %s",
                InterfaceName.ProductLine, InterfaceName.ProductFamily, InterfaceName.ProductCategory, tableName);
        log.info("query for getProductHierarchy " + query);
        List<Map<String, Object>> retList = redshiftJdbcTemplate
                .queryForList("SELECT productid, productline, productfamily, productcategory FROM " + tableName);
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

}
