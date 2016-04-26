package com.latticeengines.microservice.scoring;

import org.springframework.stereotype.Component;

import springfox.documentation.RequestHandler;
import springfox.documentation.builders.RequestHandlerSelectors;

import com.google.common.base.Predicate;
import com.latticeengines.microservice.exposed.DocketProvider;
import com.latticeengines.microservice.exposed.DocketProviderBase;

@Component("docketProvider")
public class DocketProviderImpl extends DocketProviderBase implements DocketProvider {

    protected String moduleName() {
        return "Scoring";
    }

    protected Predicate<RequestHandler> apiSelector() {
        return RequestHandlerSelectors.basePackage("com.latticeengines.scoring.controller");
    }

    protected String contextPath()  {
        return "/";
    }

}
