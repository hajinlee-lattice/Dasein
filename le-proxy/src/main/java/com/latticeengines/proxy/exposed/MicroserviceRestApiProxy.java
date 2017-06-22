package com.latticeengines.proxy.exposed;

import com.latticeengines.common.exposed.util.PropertyUtils;

public abstract class MicroserviceRestApiProxy extends BaseRestApiProxy {

    protected MicroserviceRestApiProxy(String rootpath, Object... urlVariables) {
        super(PropertyUtils.getProperty("common.microservice.url"), rootpath, urlVariables);
    }

}
