package com.latticeengines.microservice.cdl;

import org.springframework.stereotype.Component;

import com.google.common.base.Predicate;
import com.latticeengines.microservice.exposed.DocketProvider;
import com.latticeengines.microservice.exposed.DocketProviderBase;

import springfox.documentation.RequestHandler;
import springfox.documentation.builders.RequestHandlerSelectors;

@Component("docketProvider")
public class DocketProviderImpl extends DocketProviderBase implements DocketProvider {

    protected String moduleName() {
        return "CDL";
    }

    protected Predicate<RequestHandler> apiSelector() {
        return RequestHandlerSelectors.basePackage("com.latticeengines.apps.cdl.controller");
    }

    protected String contextPath()  {
        return "/";
    }

}
