package com.latticeengines.microservice.workflowapi;

import org.springframework.stereotype.Component;

import com.latticeengines.microservice.exposed.AppInfoProvider;
import com.mangofactory.swagger.models.dto.ApiInfo;

@Component("appInfoProvider")
public class AppProvider extends AppInfoProvider {

    @Override
    public ApiInfo apiInfo() {
        return new ApiInfo("Lattice Engines WORKFLOW REST API", //
                "This is the REST API exposed for the workflow api service.", //
                "termsofservice.html", //
                "bnguyen@lattice-engines.com", //
                "License", //
                "http://www.apache.org/licenses/LICENSE-2.0");
    }

}
