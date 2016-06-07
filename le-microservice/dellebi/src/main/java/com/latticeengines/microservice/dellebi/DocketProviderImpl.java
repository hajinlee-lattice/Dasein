package com.latticeengines.microservice.dellebi;

import org.springframework.stereotype.Component;

import springfox.documentation.RequestHandler;
import springfox.documentation.builders.RequestHandlerSelectors;

import com.google.common.base.Predicate;
import com.latticeengines.microservice.exposed.DocketProvider;
import com.latticeengines.microservice.exposed.DocketProviderBase;

@Component("docketProvider")
public class DocketProviderImpl extends DocketProviderBase implements DocketProvider {

    protected String moduleName() {
        return "dellebi";
    }

    protected Predicate<RequestHandler> apiSelector() {
        return RequestHandlerSelectors.basePackage("com.latticeengines.quartzclient.exposed");
    }

    protected String contextPath()  {
        return "/";
    }

}
