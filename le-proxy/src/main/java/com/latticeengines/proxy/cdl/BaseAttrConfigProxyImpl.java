package com.latticeengines.proxy.cdl;

import static com.latticeengines.proxy.exposed.ProxyUtils.shortenCustomerSpace;

import java.net.URLEncoder;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.lang.NonNull;
import org.springframework.lang.Nullable;

import com.google.common.annotations.VisibleForTesting;
import com.latticeengines.domain.exposed.query.BusinessEntity;
import com.latticeengines.domain.exposed.serviceapps.core.AttrConfig;
import com.latticeengines.domain.exposed.serviceapps.core.AttrConfigCategoryOverview;
import com.latticeengines.domain.exposed.serviceapps.core.AttrConfigRequest;
import com.latticeengines.domain.exposed.serviceapps.core.AttrConfigUpdateMode;
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
            url.append("?render=0");
        }
        AttrConfigRequest result = getKryo("get attr config by entity", url.toString(), AttrConfigRequest.class);
        result.fixJsonDeserialization();
        return result;
    }

    @SuppressWarnings({ "unchecked" })
    public Map<String, AttrConfigCategoryOverview<?>> getAttrConfigOverview(String customerSpace,
            @Nullable List<String> categoryNames, @NonNull List<String> propertyNames, boolean activeOnly) {
        return getAttrConfigOverview(customerSpace, categoryNames, propertyNames, activeOnly, null);
    }

    @SuppressWarnings({ "unchecked" })
    public Map<String, AttrConfigCategoryOverview<?>> getAttrConfigOverview(String customerSpace, @Nullable List<String> categoryNames,
                                                                            @NonNull List<String> propertyNames,
                                                                            boolean activeOnly, String attributeSetName) {
        log.info("customerSpace is " + customerSpace + ", categoryName is " + categoryNames + ", propertyName is "
                + propertyNames + " activeOnly is " + activeOnly + " attributeSetName is " + attributeSetName);
        String url = contructUrlForGetAttrConfigOverview(customerSpace, categoryNames, propertyNames, activeOnly, attributeSetName);
        log.info("getAttrConfigOverview url is " + url);
        return postKryo("get Attribute Configuration Overview", url, propertyNames, Map.class);
    }

    String contructUrlForGetAttrConfigOverview(String customerSpace, @Nullable List<String> categoryNames,
                                               @NonNull List<String> propertyNames, boolean activeOnly) {
        return contructUrlForGetAttrConfigOverview(customerSpace, categoryNames, propertyNames, activeOnly, null);
    }

    @VisibleForTesting
    String contructUrlForGetAttrConfigOverview(String customerSpace, @Nullable List<String> categoryNames,
            @NonNull List<String> propertyNames, boolean activeOnly, String attributeSetName) {
        StringBuilder url = new StringBuilder();
        url.append(constructUrl("/customerspaces/{customerSpace}/attrconfig/overview", //
                shortenCustomerSpace(customerSpace)));
        if (categoryNames != null) {
            url.append("?");
            for (String categoryName : categoryNames) {
                try {
                    url.append("category=").append(URLEncoder.encode(categoryName, "UTF-8")).append("&");
                } catch (Exception ex) {
                    log.warn("Can not encode for category=" + categoryName + " error=" + ex.getMessage());
                    url.append("category=").append(categoryName).append("&");
                }
            }
        }
        if (!url.toString().endsWith("&")) {
            url.append("?");
        }
        url.append("activeOnly=").append(activeOnly);
        if (StringUtils.isNotEmpty(attributeSetName)) {
            url.append("&attributeSetName=").append(attributeSetName);
        }
        return url.toString();
    }

    public AttrConfigRequest getAttrConfigByCategory(String customerSpace, String categoryName) {
        return getAttrConfigByCategory(customerSpace, categoryName, null);
    }

    public AttrConfigRequest getAttrConfigByCategory(String customerSpace, String categoryName, String attributeSetName) {
        String url = constructUrl("/customerspaces/{customerSpace}/attrconfig/categories/", //
                shortenCustomerSpace(customerSpace));
        url += categoryName;
        List<String> params = new ArrayList<>();
        if (StringUtils.isNotEmpty(attributeSetName)) {
            params.add("attributeSetName=" + attributeSetName);
        }
        if (!params.isEmpty()) {
            url += "?" + StringUtils.join(params, "&");
        }
        log.info("getAttrConfigByCategory url is " + url);
        AttrConfigRequest result = getKryo("get attr config by category", url, AttrConfigRequest.class);
        result.fixJsonDeserialization();
        return result;
    }

    public AttrConfigRequest saveAttrConfig(String customerSpace, AttrConfigRequest request,
            AttrConfigUpdateMode mode) {
        String url = constructUrl("/customerspaces/{customerSpace}/attrconfig/?mode={mode}", //
                shortenCustomerSpace(customerSpace), mode);
        AttrConfigRequest result = post("save attr config", url, request, AttrConfigRequest.class);
        result.fixJsonDeserialization();
        return result;
    }

    public AttrConfigRequest validateAttrConfig(String customerSpace, AttrConfigRequest request,
            AttrConfigUpdateMode mode) {
        String url = constructUrl("/customerspaces/{customerSpace}/attrconfig/validate/?mode={mode}", //
                shortenCustomerSpace(customerSpace), mode);
        AttrConfigRequest result = post("validate attr config request", url, request, AttrConfigRequest.class);
        result.fixJsonDeserialization();
        return result;
    }

    @SuppressWarnings("unchecked")
    public Map<BusinessEntity, List<AttrConfig>> getCustomDisplayNames(String customerSpace) {
        String url = constructUrl("/customerspaces/{customerSpace}/attrconfig/custom-displaynames", //
                shortenCustomerSpace(customerSpace));
        log.info("getCustomDisplayNames url is " + url);
        Map<BusinessEntity, List<AttrConfig>> result = getKryo("get custom displayNames", url, Map.class);
        return result;
    }

    public void removeAttrConfigByTenantAndEntity(String customerSpace, BusinessEntity entity) {
        String url = constructUrl("/customerspaces/{customerSpace}/attrconfig/?entity={entity}", //
                shortenCustomerSpace(customerSpace), entity);
        log.info("removeAttrConfigByTenant url is " + url);
        delete("remove config by tenant ", url);

    }

    public AttrConfigRequest getAttrConfigByProperty(String customerSpace, String propertyName, boolean enabled) {
        StringBuilder url = new StringBuilder();
        url.append(constructUrl("/customerspaces/{customerSpace}/attrconfig/properties/{propertyName}", //
                shortenCustomerSpace(customerSpace), propertyName));
        if (!enabled) {
            url.append("?enabled=0");
        }
        AttrConfigRequest result = getKryo("get attr config by Property", url.toString(), AttrConfigRequest.class);
        result.fixJsonDeserialization();
        return result;
    }

}
