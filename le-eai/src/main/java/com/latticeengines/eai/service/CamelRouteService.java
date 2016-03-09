package com.latticeengines.eai.service;

import org.apache.camel.builder.RouteBuilder;

import com.latticeengines.domain.exposed.eai.route.CamelRouteConfiguration;

public interface CamelRouteService<T extends CamelRouteConfiguration> {

    RouteBuilder generateRoute(CamelRouteConfiguration camelRouteConfiguration);

    Boolean routeIsFinished(CamelRouteConfiguration camelRouteConfiguration);

    Double getProgress(CamelRouteConfiguration camelRouteConfiguration);

}
