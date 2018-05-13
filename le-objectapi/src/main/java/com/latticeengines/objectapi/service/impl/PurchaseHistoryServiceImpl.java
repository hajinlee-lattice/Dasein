package com.latticeengines.objectapi.service.impl;

import java.util.ArrayList;
import java.util.Collections;
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
        String tableName = dataCollectionProxy.getTableName(tenant.getId(),
                BusinessEntity.PeriodTransaction.getServingStore());
        log.info(String.format(
                "Get Period Transaction table %s for %s with account %s and periodName %s, productType %s, version %s",
                tableName, tenant.getId(), accountId, periodName, productType, version));
        // TODO add productType to where clause to filter out bundle
        // transactions
        List<Map<String, Object>> retList = redshiftJdbcTemplate.queryForList(
                "SELECT periodid, productid, totalamount, totalquantity, transactioncount FROM " + tableName
                        + " where accountid = ? and periodname = ?",
                new Object[] { accountId, periodName });
        for (Map row : retList) {
            PeriodTransaction periodTransaction = new PeriodTransaction();
            periodTransaction.setTotalAmount((Double) row.get("totalamount"));
            periodTransaction.setTotalQuantity((Long) row.get("totalquantity"));
            periodTransaction.setTransactionCount((Double) row.get("transactioncount"));
            periodTransaction.setProductId((String) row.get("productid"));
            periodTransaction.setPeriodId((Integer) row.get("periodid"));
            resultList.add(periodTransaction);
        }
        log.info("resultList is " + resultList);
        return resultList;
    }

    @Override
    public List<PeriodTransaction> getPeriodTransactionForSegmentAccount(String spendanalyticssegmentId,
            String periodName) {
        return Collections.emptyList();
    }

    @SuppressWarnings("rawtypes")
    @Override
    public List<ProductHierarchy> getProductHierarchy(DataCollection.Version version) {
        List<ProductHierarchy> resultList = new ArrayList<>();
        Tenant tenant = MultiTenantContext.getTenant();
        String tableName = dataCollectionProxy.getTableName(tenant.getId(),
                BusinessEntity.ProductHierarchy.getServingStore());
        log.info(String.format("Get product Hierarchy table %s for %s", tableName, tenant.getId()));
        List<Map<String, Object>> retList = redshiftJdbcTemplate
                .queryForList("SELECT productid, productline, productfamily, productcategory FROM " + tableName);
        for (Map row : retList) {
            ProductHierarchy productHierarchy = new ProductHierarchy();
            productHierarchy.setProductCategory((String) row.get("productcategory"));
            productHierarchy.setProductFamily((String) row.get("productfamily"));
            productHierarchy.setProductLine((String) row.get("productline"));
            productHierarchy.setProductId((String) row.get("productid"));
            resultList.add(productHierarchy);
        }
        log.info("resultList is " + resultList);
        return resultList;
    }

}
