package com.latticeengines.proxy.cdl;

import static com.latticeengines.proxy.exposed.ProxyUtils.shortenCustomerSpace;

import java.util.List;

import javax.inject.Inject;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.springframework.stereotype.Component;

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
        if (version != null) {
            url += "?version=" + version.toString();
            if (CollectionUtils.isNotEmpty(groups)) {
                url += "&groups=" + StringUtils.join(groups, ",");
            }
        } else {
            if (CollectionUtils.isNotEmpty(groups)) {
                url += "?groups=" + StringUtils.join(groups, ",");
            }
        }
        List<ColumnMetadata> list = getList("serving store metadata", url, ColumnMetadata.class);
        if (CollectionUtils.isNotEmpty(list)) {
            return Flux.fromIterable(list);
        } else {
            return Flux.empty();
        }
    }

    @Override
    public Flux<ColumnMetadata> getNewModelingAttrs(String customerSpace) {
        String url = constructUrl("/customerspaces/{customerSpace}/servingstore/new-modeling",
                shortenCustomerSpace(customerSpace));
        List<ColumnMetadata> list = getList("serving store new modeling", url, ColumnMetadata.class);
        if (CollectionUtils.isNotEmpty(list)) {
            return Flux.fromIterable(list);
        } else {
            return Flux.empty();
        }
    }

    @Override
    public Flux<ColumnMetadata> getNewModelingAttrs(String customerSpace, Version version) {
        String url = constructUrl("/customerspaces/{customerSpace}/servingstore/new-modeling",
                shortenCustomerSpace(customerSpace));
        if (version != null) {
            url += "?version=" + version.toString();
        }
        List<ColumnMetadata> list = getList("serving store new modeling", url, ColumnMetadata.class);
        if (CollectionUtils.isNotEmpty(list)) {
            return Flux.fromIterable(list);
        } else {
            return Flux.empty();
        }
    }

}
