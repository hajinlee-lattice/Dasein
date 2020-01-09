package com.latticeengines.proxy.objectapi;

import java.io.UnsupportedEncodingException;
import java.net.URLDecoder;
import java.util.List;

import javax.inject.Inject;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.cache.annotation.Cacheable;
import org.springframework.context.annotation.Scope;
import org.springframework.context.annotation.ScopedProxyMode;
import org.springframework.stereotype.Component;

import com.google.common.annotations.VisibleForTesting;
import com.latticeengines.domain.exposed.cache.CacheName;
import com.latticeengines.domain.exposed.metadata.DataCollection;
import com.latticeengines.domain.exposed.metadata.transaction.ProductType;
import com.latticeengines.domain.exposed.query.DataPage;
import com.latticeengines.domain.exposed.ulysses.PeriodTransaction;
import com.latticeengines.domain.exposed.ulysses.ProductHierarchy;
import com.latticeengines.proxy.exposed.MicroserviceRestApiProxy;
import com.latticeengines.proxy.exposed.ProxyUtils;
import com.latticeengines.proxy.exposed.objectapi.PeriodTransactionProxy;

@Component("periodTransactionProxy")
@Scope(proxyMode = ScopedProxyMode.TARGET_CLASS)
public class PeriodTransactionProxyImpl extends MicroserviceRestApiProxy implements PeriodTransactionProxy {

    private static final Logger log = LoggerFactory.getLogger(PeriodTransactionProxyImpl.class);

    private final PeriodTransactionProxyImpl _periodTransactionProxyImpl;

    @Inject
    public PeriodTransactionProxyImpl(PeriodTransactionProxyImpl periodTransactionProxy) {
        super("objectapi");
        this._periodTransactionProxyImpl = periodTransactionProxy;
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
        return _periodTransactionProxyImpl.getPeriodTransactionsForSegmentAccountsFromCache(customerSpace,
                spendAnalyticsSegment, periodName);
    }

    @Override
    public List<ProductHierarchy> getProductHierarchy(String customerSpace, DataCollection.Version version) {
        return _periodTransactionProxyImpl.getProductHierarchyFromCache(customerSpace, version);
    }

    @Override
    public DataPage getAllSpendAnalyticsSegments(String customerSpace) {
        return _periodTransactionProxyImpl.getAllSpendAnalyticsSegmentsFromCache(customerSpace);
    }

    @Override
    public List<String> getFinalAndFirstTransactionDate(String customerSpace) {
        String url = constructUrl("/customerspaces/{customerSpace}/periodtransactions/transaction/maxmindate",
                ProxyUtils.shortenCustomerSpace(customerSpace));

        log.info("getFinalAndFirstTransactionDate url " + url);
        return getList("getFinalAndFirstTransactionDate", url, String.class);
    }

    @Cacheable(cacheNames = CacheName.Constants.ObjectApiCacheName, key = "T(java.lang.String).format(\"%s|%s|%s|SpAnSeg_Txn_Data\", T(com.latticeengines.proxy.exposed.ProxyUtils).shortenCustomerSpace(#customerSpace), #spendAnalyticsSegment, #periodName)")
    public List<PeriodTransaction> getPeriodTransactionsForSegmentAccountsFromCache(String customerSpace,
            String spendAnalyticsSegment, String periodName) {
        String url = constructUrl(
                "/customerspaces/{customerSpace}/periodtransactions/spendanalyticssegment/{spendAnalyticsSegment}",
                ProxyUtils.shortenCustomerSpace(customerSpace), spendAnalyticsSegment);
        try {
            url = URLDecoder.decode(url, "UTF-8");
        } catch (UnsupportedEncodingException e) {
            throw new RuntimeException(e);
        }
        if (periodName != null) {
            url += ("?periodname=" + periodName);
        }
        log.info("Missed cache for getPeriodTransactionsForSegmentAccounts url " + url);
        return getList("getPeriodTransactionsForSegmentAccounts", url, PeriodTransaction.class);
    }

    @Cacheable(cacheNames = CacheName.Constants.ObjectApiCacheName, key = "T(java.lang.String).format(\"%s|Spend_Analytics_Segments\", T(com.latticeengines.proxy.exposed.ProxyUtils).shortenCustomerSpace(#customerSpace))")
    public DataPage getAllSpendAnalyticsSegmentsFromCache(String customerSpace) {
        String url = constructUrl("/customerspaces/{customerSpace}/periodtransactions/spendanalyticssegments",
                ProxyUtils.shortenCustomerSpace(customerSpace));
        log.info("Missed cache for getProductHierarchy url " + url);
        return get("getProductHierarchy", url, DataPage.class);
    }

    @Cacheable(cacheNames = CacheName.Constants.ObjectApiCacheName, key = "T(java.lang.String).format(\"%s|%s|Product_Hierarchy\", T(com.latticeengines.proxy.exposed.ProxyUtils).shortenCustomerSpace(#customerSpace), #version)")
    public List<ProductHierarchy> getProductHierarchyFromCache(String customerSpace, DataCollection.Version version) {
        String url = constructUrl("/customerspaces/{customerSpace}/periodtransactions/producthierarchy",
                ProxyUtils.shortenCustomerSpace(customerSpace));
        if (version != null) {
            url += ("?version=" + version);
        }
        log.info("Missed cache for getProductHierarchy url " + url);
        return getList("getProductHierarchy", url, ProductHierarchy.class);
    }
}
