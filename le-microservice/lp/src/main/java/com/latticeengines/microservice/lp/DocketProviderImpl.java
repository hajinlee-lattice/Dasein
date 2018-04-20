package com.latticeengines.microservice.lp;

import org.springframework.stereotype.Component;

import com.google.common.base.Predicate;
import com.latticeengines.common.exposed.util.SwaggerUtils;
import com.latticeengines.microservice.exposed.DocketProvider;
import com.latticeengines.microservice.exposed.DocketProviderBase;

import springfox.documentation.RequestHandler;

@Component("docketProvider")
public class DocketProviderImpl extends DocketProviderBase implements DocketProvider {

    protected String moduleName() {
        return "LP";
    }

    protected Predicate<RequestHandler> apiSelector() {
        return SwaggerUtils.getApiSelector("com.latticeengines.apps.lp.controller.*",
                "com.latticeengines.apps.core.controller.*");
    }

    protected String contextPath()  {
        return "/";
    }

}
