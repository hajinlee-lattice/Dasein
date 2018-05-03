package com.latticeengines.proxy.cdl;

import static com.latticeengines.proxy.exposed.ProxyUtils.shortenCustomerSpace;

import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.lang.NonNull;
import org.springframework.lang.Nullable;

import com.google.common.annotations.VisibleForTesting;
import com.latticeengines.domain.exposed.query.BusinessEntity;
import com.latticeengines.domain.exposed.serviceapps.core.AttrConfigCategoryOverview;
import com.latticeengines.domain.exposed.serviceapps.core.AttrConfigOverview;
import com.latticeengines.domain.exposed.serviceapps.core.AttrConfigRequest;
import com.latticeengines.proxy.exposed.MicroserviceRestApiProxy;

public abstract class BaseAttrConfigProxyImpl extends MicroserviceRestApiProxy {

    private static final Logger log = LoggerFactory.getLogger(BaseAttrConfigProxyImpl.class);

    protected BaseAttrConfigProxyImpl(String rootpath, Object... urlVariables) {
        super(rootpath, urlVariables);
    }

    public AttrConfigRequest getAttrConfigByEntity(String customerSpace, BusinessEntity entity, boolean render) {
        StringBuilder url = new StringBuilder();
        url.append(constructUrl("/customerspaces/{customerSpace}/attrconfig/entities/{entity}", //
                shortenCustomerSpace(customerSpace), entity));
        if (!render) {
            url.append("?render=" + render);
        }
        return getKryo("get attr config by entity", url.toString(), AttrConfigRequest.class);
    }

    @SuppressWarnings({ "unchecked" })
    public List<AttrConfigOverview<?>> getAttrConfigOverview(String customerSpace, String categoryName,
            @NonNull String propertyName) {
        log.info("customerSpace is " + customerSpace + ", categoryName is " + categoryName + ", propertyName is "
                + propertyName);
        StringBuilder url = new StringBuilder();
        url.append(constructUrl("/customerspaces/{customerSpace}/attrconfig", //
                shortenCustomerSpace(customerSpace)));
        url.append("?property=" + propertyName);
        if (categoryName != null) {
            url.append("&category=" + categoryName);
        }
        log.info("url is " + url);
        return getKryo("get Attribute Configuration Overview", url.toString(), List.class);
    }

    @SuppressWarnings({ "unchecked" })
    public Map<String, AttrConfigCategoryOverview<?>> getAttrConfigOverview(String customerSpace,
            @Nullable List<String> categoryNames, @NonNull List<String> propertyNames, boolean activeOnly) {
        log.info("customerSpace is " + customerSpace + ", categoryName is " + categoryNames + ", propertyName is "
                + propertyNames + " activeOnly " + activeOnly);
        String url = contructUrlForGetAttrConfigOverview(customerSpace, categoryNames, propertyNames, activeOnly);
        log.info("url is " + url);
        return postKryo("get Attribute Configuration Overview", url.toString(), propertyNames, Map.class);
    }

    @VisibleForTesting
    String contructUrlForGetAttrConfigOverview(String customerSpace, @Nullable List<String> categoryNames,
            @NonNull List<String> propertyNames, boolean activeOnly) {
        StringBuilder url = new StringBuilder();
        url.append(constructUrl("/customerspaces/{customerSpace}/attrconfig/overview", //
                shortenCustomerSpace(customerSpace)));
        if (categoryNames != null) {
            url.append("?");
            for (String categoryName : categoryNames) {
                url.append("category=" + categoryName + "&");
            }
        }
        if (!url.toString().endsWith("&")) {
            url.append("?");
        }
        url.append("activeOnly=" + activeOnly);
        log.info("url is " + url);
        return url.toString();
    }

    public AttrConfigRequest getAttrConfigByCategory(String customerSpace, String categoryName) {
        String url = constructUrl("/customerspaces/{customerSpace}/attrconfig/categories/{categoryName}", //
                shortenCustomerSpace(customerSpace), categoryName);
        return getKryo("get attr config by category", url, AttrConfigRequest.class);
    }

    public AttrConfigRequest saveAttrConfig(String customerSpace, AttrConfigRequest request) {
        String url = constructUrl("/customerspaces/{customerSpace}/attrconfig/", //
                shortenCustomerSpace(customerSpace));
        return post("save attr config", url, request, AttrConfigRequest.class);
    }

    public AttrConfigRequest validateAttrConfig(String customerSpace, AttrConfigRequest request) {
        String url = constructUrl("/customerspaces/{customerSpace}/attrconfig/validate", //
                shortenCustomerSpace(customerSpace));
        return post("validate attr config request", url, request, AttrConfigRequest.class);
    }

}
