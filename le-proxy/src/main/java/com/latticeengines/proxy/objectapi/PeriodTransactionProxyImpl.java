package com.latticeengines.proxy.objectapi;

import java.util.List;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import com.google.common.annotations.VisibleForTesting;
import com.latticeengines.domain.exposed.metadata.DataCollection;
import com.latticeengines.domain.exposed.metadata.transaction.ProductType;
import com.latticeengines.domain.exposed.query.DataPage;
import com.latticeengines.domain.exposed.ulysses.PeriodTransaction;
import com.latticeengines.domain.exposed.ulysses.ProductHierarchy;
import com.latticeengines.proxy.exposed.MicroserviceRestApiProxy;
import com.latticeengines.proxy.exposed.ProxyUtils;
import com.latticeengines.proxy.exposed.objectapi.PeriodTransactionProxy;

@Component("periodTransactionProxy")
public class PeriodTransactionProxyImpl extends MicroserviceRestApiProxy implements PeriodTransactionProxy {

    private static final Logger log = LoggerFactory.getLogger(PeriodTransactionProxyImpl.class);

    public PeriodTransactionProxyImpl() {
        super("/objectapi");
    }

    @Override
    public List<PeriodTransaction> getPeriodTransactionsByAccountId(String customerSpace, String accountId,
            String periodName, ProductType productType) {
        String url = constructGetPeriodTransactionByAccountId(customerSpace, accountId, periodName, productType);
        log.info("getPeriodTransactionByAccountId url " + url);
        return getList("getPeriodTransactionByAccountId", url, PeriodTransaction.class);
    }

    @VisibleForTesting
    String constructGetPeriodTransactionByAccountId(String customerSpace, String accountId, String periodName,
            ProductType productType) {
        String url = constructUrl("/customerspaces/{customerSpace}/periodtransactions/accountid/{accountId}",
                ProxyUtils.shortenCustomerSpace(customerSpace), accountId);
        StringBuilder sb = new StringBuilder();
        if (periodName != null) {
            sb.append("periodname=").append(periodName).append("&");
        }
        if (productType != null) {
            sb.append("producttype=").append(productType).append("&");
        }
        if (StringUtils.isNotEmpty(sb.toString())) {
            url += "?";
            url += sb.subSequence(0, sb.length() - 1).toString();
        }
        return url;
    }

    @Override
    public List<PeriodTransaction> getPeriodTransactionsForSegmentAccounts(String customerSpace,
            String spendAnalyticsSegment, String periodName) {
        String url = constructUrl(
                "/customerspaces/{customerSpace}/periodtransactions/spendanalyticssegment/{spendAnalyticsSegment}",
                ProxyUtils.shortenCustomerSpace(customerSpace), spendAnalyticsSegment);
        if (periodName != null) {
            url += ("?periodname=" + periodName);
        }
        log.info("getPeriodTransactionsForSegmentAccounts url " + url);
        return getList("getPeriodTransactionsForSegmentAccounts", url, PeriodTransaction.class);
    }

    @Override
    public List<ProductHierarchy> getProductHierarchy(String customerSpace, DataCollection.Version version) {
        String url = constructUrl("/customerspaces/{customerSpace}/periodtransactions/producthierarchy",
                ProxyUtils.shortenCustomerSpace(customerSpace));
        if (version != null) {
            url += ("?version=" + version);
        }
        log.info("getProductHierarchy url " + url);
        return getList("getProductHierarchy", url, ProductHierarchy.class);
    }

    @Override
    public DataPage getAllSpendAnalyticsSegments(String customerSpace) {
        String url = constructUrl("/customerspaces/{customerSpace}/periodtransactions/spendanalyticssegments",
                ProxyUtils.shortenCustomerSpace(customerSpace));

        log.info("getProductHierarchy url " + url);
        return get("getProductHierarchy", url, DataPage.class);
    }

    @Override
    public List<String> getFinalAndFirstTransactionDate(String customerSpace) {
        String url = constructUrl("/customerspaces/{customerSpace}/periodtransactions/transaction/maxmindate",
                ProxyUtils.shortenCustomerSpace(customerSpace));

        log.info("getFinalAndFirstTransactionDate url " + url);
        return getList("getFinalAndFirstTransactionDate", url, String.class);
    }
}
