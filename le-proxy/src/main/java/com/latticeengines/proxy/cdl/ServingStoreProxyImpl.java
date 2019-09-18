package com.latticeengines.proxy.cdl;

import static com.latticeengines.proxy.exposed.ProxyUtils.shortenCustomerSpace;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import javax.inject.Inject;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.springframework.stereotype.Component;

import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.metadata.ColumnMetadata;
import com.latticeengines.domain.exposed.metadata.DataCollection;
import com.latticeengines.domain.exposed.metadata.DataCollection.Version;
import com.latticeengines.domain.exposed.propdata.manage.ColumnSelection;
import com.latticeengines.domain.exposed.query.BusinessEntity;
import com.latticeengines.proxy.exposed.MicroserviceRestApiProxy;
import com.latticeengines.proxy.exposed.cdl.ServingStoreCacheService;
import com.latticeengines.proxy.exposed.cdl.ServingStoreProxy;

import reactor.core.publisher.Flux;

@Component
public class ServingStoreProxyImpl extends MicroserviceRestApiProxy implements ServingStoreProxy {

    @Inject
    private ServingStoreCacheService cacheService;

    protected ServingStoreProxyImpl() {
        super("cdl");
    }

    @Override
    public List<ColumnMetadata> getDecoratedMetadataFromCache(String customerSpace, BusinessEntity entity) {
        return cacheService.getDecoratedMetadata(customerSpace, entity);
    }

    @Override
    public Set<String> getServingStoreColumnsFromCache(String customerSpace, BusinessEntity entity) {
        return cacheService.getServingTableColumns(customerSpace, entity);
    }

    @Override
    public Flux<ColumnMetadata> getDecoratedMetadata(String customerSpace, BusinessEntity entity,
            List<ColumnSelection.Predefined> groups) {
        String url = constructUrl("/customerspaces/{customerSpace}/servingstore/{entity}/decoratedmetadata", //
                shortenCustomerSpace(customerSpace), entity);
        if (CollectionUtils.isNotEmpty(groups)) {
            url += "?groups=" + StringUtils.join(groups, ",");
        }
        List<ColumnMetadata> list = getList("serving store metadata", url, ColumnMetadata.class);
        if (CollectionUtils.isNotEmpty(list)) {
            return Flux.fromIterable(list);
        } else {
            return Flux.empty();
        }
    }

    @Override
    public Flux<ColumnMetadata> getDecoratedMetadata(String customerSpace, BusinessEntity entity,
                                                     List<ColumnSelection.Predefined> groups, DataCollection.Version version) {
        String url = constructUrl("/customerspaces/{customerSpace}/servingstore/{entity}/decoratedmetadata", //
                shortenCustomerSpace(customerSpace), entity);
        url += getVersionGroupParm(version, groups);
        List<ColumnMetadata> list = getList("serving store metadata", url, ColumnMetadata.class);
        if (CollectionUtils.isNotEmpty(list)) {
            return Flux.fromIterable(list);
        } else {
            return Flux.empty();
        }
    }

    private String getVersionGroupParm(DataCollection.Version version, List<ColumnSelection.Predefined> groups) {
        StringBuffer url = new StringBuffer();
        if (version != null) {
            url.append("?version=" + version.toString());
            if (CollectionUtils.isNotEmpty(groups)) {
                url.append("&groups=" + StringUtils.join(groups, ","));
            }
        } else {
            if (CollectionUtils.isNotEmpty(groups)) {
                url.append("?groups=" + StringUtils.join(groups, ","));
            }
        }
        return url.toString();
    }

    @Override
    public List<ColumnMetadata> getDecoratedMetadata(String customerSpace, List<BusinessEntity> entities,
                                                     List<ColumnSelection.Predefined> groups, DataCollection.Version version) {
        String url = constructUrl("/customerspaces/{customerSpace}/servingstore/decoratedmetadata", //
                shortenCustomerSpace(customerSpace));
        url += getVersionGroupParm(version, groups);
        List<?> list = post("serving store metadata", url, entities, List.class);
        return JsonUtils.convertList(list, ColumnMetadata.class);
    }

    @Override
    public Flux<ColumnMetadata> getNewModelingAttrs(String customerSpace) {
        return getNewModelingAttrs(customerSpace, null);
    }

    @Override
    public Flux<ColumnMetadata> getNewModelingAttrs(String customerSpace, Version version) {
        return getNewModelingAttrs(customerSpace, null, null);
    }

    @Override
    public Flux<ColumnMetadata> getNewModelingAttrs(String customerSpace, BusinessEntity entity, Version version) {
        String url = constructUrl("/customerspaces/{customerSpace}/servingstore/new-modeling",
                shortenCustomerSpace(customerSpace));
        List<String> params = new ArrayList<>();
        if (version != null) {
            params.add("version=" + version.toString());
        }
        if (entity != null) {
            params.add("entity=" + entity);
        }
        if (CollectionUtils.isNotEmpty(params)) {
            url += "?" + StringUtils.join(params, "&");
        }
        List<ColumnMetadata> list = getList("serving store new modeling", url, ColumnMetadata.class);
        if (CollectionUtils.isNotEmpty(list)) {
            return Flux.fromIterable(list);
        } else {
            return Flux.empty();
        }
    }

    @Override
    public Flux<ColumnMetadata> getAllowedModelingAttrs(String customerSpace) {
        return getAllowedModelingAttrs(customerSpace, false, null);
    }

    @Override
    public Flux<ColumnMetadata> getAllowedModelingAttrs(String customerSpace, Boolean allCustomerAttrs,
            Version version) {
        return getAllowedModelingAttrs(customerSpace, null, allCustomerAttrs, version);
    }

    @Override
    public Flux<ColumnMetadata> getAllowedModelingAttrs(String customerSpace, BusinessEntity entity,
            Boolean allCustomerAttrs, Version version) {
        String url = constructUrl("/customerspaces/{customerSpace}/servingstore/allow-modeling",
                shortenCustomerSpace(customerSpace));
        List<String> params = new ArrayList<>();
        if (version != null) {
            params.add("version=" + version.toString());
        }
        if (Boolean.TRUE.equals(allCustomerAttrs)) {
            params.add("all-customer-attrs=1");
        }
        if (entity != null) {
            params.add("entity=" + entity);
        }
        if (CollectionUtils.isNotEmpty(params)) {
            url += "?" + StringUtils.join(params, "&");
        }
        List<ColumnMetadata> list = getList("serving store allowed modeling", url, ColumnMetadata.class);
        if (CollectionUtils.isNotEmpty(list)) {
            return Flux.fromIterable(list);
        } else {
            return Flux.empty();
        }
    }

}
