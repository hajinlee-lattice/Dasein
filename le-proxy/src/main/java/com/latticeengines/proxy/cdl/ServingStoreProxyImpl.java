package com.latticeengines.proxy.cdl;

import static com.latticeengines.proxy.exposed.ProxyUtils.shortenCustomerSpace;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import javax.inject.Inject;

import com.latticeengines.proxy.exposed.cdl.ServingStoreCacheService;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.springframework.stereotype.Component;

import com.latticeengines.domain.exposed.metadata.ColumnMetadata;
import com.latticeengines.domain.exposed.propdata.manage.ColumnSelection;
import com.latticeengines.domain.exposed.query.BusinessEntity;
import com.latticeengines.proxy.exposed.MicroserviceRestApiProxy;
import com.latticeengines.proxy.exposed.cdl.ServingStoreProxy;

import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

@Component
public class ServingStoreProxyImpl extends MicroserviceRestApiProxy implements ServingStoreProxy {

    @Inject
    private ServingStoreCacheService cacheService;

    protected ServingStoreProxyImpl() {
        super("cdl");
    }

    @Override
    public Mono<Long> getDecoratedMetadataCount(String customerSpace, BusinessEntity entity) {
        return getDecoratedMetadataCount(customerSpace, entity, Collections.emptyList());
    }

    @Override
    public Flux<ColumnMetadata> getDecoratedMetadata(String customerSpace, BusinessEntity entity) {
        return getDecoratedMetadata(customerSpace, entity, Collections.emptyList());
    }

    @Override
    public List<ColumnMetadata> getDecoratedMetadataFromCache(String customerSpace, BusinessEntity entity) {
        return cacheService.getDecoratedMetadata(customerSpace, entity);
    }

    @Override
    public Mono<Long> getDecoratedMetadataCount(String customerSpace, BusinessEntity entity, List<ColumnSelection.Predefined> groups) {
        String url = constructUrl("/customerspaces/{customerSpace}/servingstore/{entity}/decoratedmetadata/count", //
                shortenCustomerSpace(customerSpace), entity);
        if (CollectionUtils.isNotEmpty(groups)) {
            url += "?groups=" + StringUtils.join(groups, ",");
        }
        return getMono("serving store metadata count", url, Long.class);
    }

    @Override
    public Flux<ColumnMetadata> getDecoratedMetadata(String customerSpace, BusinessEntity entity, List<ColumnSelection.Predefined> groups) {
        String url = constructUrl("/customerspaces/{customerSpace}/servingstore/{entity}/decoratedmetadata", //
                shortenCustomerSpace(customerSpace), entity);
        List<String> params = new ArrayList<>();
        if (CollectionUtils.isNotEmpty(groups)) {
            url += "?groups=" + StringUtils.join(groups, ",");
        }
        return getFlux("serving store metadata", url, ColumnMetadata.class);
    }

}
