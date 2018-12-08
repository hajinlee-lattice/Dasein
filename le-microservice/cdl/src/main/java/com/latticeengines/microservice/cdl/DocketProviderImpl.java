package com.latticeengines.microservice.cdl;

import java.util.function.Predicate;

import org.springframework.stereotype.Component;

import com.latticeengines.common.exposed.util.SwaggerUtils;
import com.latticeengines.microservice.exposed.DocketProvider;
import com.latticeengines.microservice.exposed.DocketProviderBase;

import springfox.documentation.RequestHandler;

@Component("docketProvider")
public class DocketProviderImpl extends DocketProviderBase implements DocketProvider {

    protected String moduleName() {
        return "CDL";
    }

    protected Predicate<RequestHandler> apiSelector() {
        return SwaggerUtils.getApiSelector("com.latticeengines.apps.cdl.controller.*",
                "com.latticeengines.apps.core.controller.*");
    }

    protected String contextPath()  {
        return "/";
    }

}
