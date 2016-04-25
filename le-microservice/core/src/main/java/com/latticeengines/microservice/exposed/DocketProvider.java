package com.latticeengines.microservice.exposed;

import springfox.documentation.spring.web.plugins.Docket;

public interface DocketProvider {

    Docket getDocket();

}
