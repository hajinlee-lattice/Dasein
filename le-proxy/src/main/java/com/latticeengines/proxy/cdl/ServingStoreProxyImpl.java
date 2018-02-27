package com.latticeengines.proxy.cdl;

import static com.latticeengines.proxy.exposed.ProxyUtils.shortenCustomerSpace;

import org.springframework.stereotype.Component;

import com.latticeengines.domain.exposed.metadata.ColumnMetadata;
import com.latticeengines.domain.exposed.query.BusinessEntity;
import com.latticeengines.proxy.exposed.MicroserviceRestApiProxy;
import com.latticeengines.proxy.exposed.cdl.ServingStoreProxy;

import reactor.core.publisher.Flux;

@Component
public class ServingStoreProxyImpl extends MicroserviceRestApiProxy implements ServingStoreProxy {

    protected ServingStoreProxyImpl() {
        super("cdl");
    }

    @Override
    public Flux<ColumnMetadata> getDecoratedMetadata(String customerSpace, BusinessEntity entity) {
        String url = constructUrl("/customerspaces/{customerSpace}/servingstore/{entity}/decoratedmetadata", //
                shortenCustomerSpace(customerSpace), entity);
        return getFlux("serving store metadata", url, ColumnMetadata.class);
    }

}
