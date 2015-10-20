package com.latticeengines.microservice.propdata;

import com.latticeengines.microservice.exposed.AppInfoProvider;
import com.mangofactory.swagger.models.dto.ApiInfo;
import org.springframework.stereotype.Component;

@Component("appInfoProvider")
public class AppProvider extends AppInfoProvider {

    @Override
    public ApiInfo apiInfo() {
        return new ApiInfo("Lattice Engines PropData REST API", //
                "This is the REST API exposed for the PropData service.", //
                "termsofservice.html", //
                "rgonzalez@lattice-engines.com", //
                "License", //
                "http://www.apache.org/licenses/LICENSE-2.0");
    }

}
