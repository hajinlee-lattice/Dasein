package com.latticeengines.microservice.objectapi;

import java.util.function.Predicate;

import org.springframework.stereotype.Component;

import com.latticeengines.microservice.exposed.DocketProvider;
import com.latticeengines.microservice.exposed.DocketProviderBase;

import springfox.documentation.RequestHandler;
import springfox.documentation.builders.RequestHandlerSelectors;

@Component("docketProvider")
public class DocketProviderImpl extends DocketProviderBase implements DocketProvider {

    protected String moduleName() {
        return "Object";
    }

    protected Predicate<RequestHandler> apiSelector() {
        return RequestHandlerSelectors.basePackage("com.latticeengines.objectapi.controller")::apply;
    }

    protected String contextPath()  {
        return "/";
    }

}
