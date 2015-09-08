package com.latticeengines.microservice.doc;

import com.latticeengines.microservice.exposed.AppInfoProvider;

import org.springframework.stereotype.Component;

import com.mangofactory.swagger.models.dto.ApiInfo;

@Component("appInfoProvider")
public class AppProvider extends AppInfoProvider {

    @Override
    public ApiInfo apiInfo() {
        return new ApiInfo(
                "Lattice Engines Microservice REST APIs", //
                "This is the parent of REST API exposed for the all microservices, for specific API documentation"
                        + " of a single microservice, replace 'doc/doc' in the search box abvoe with the name of the microservice project,"
                        + " e.g. eai, modeling.", //
                "termsofservice.html", //
                "rgonzalez@lattice-engines.com", //
                "License", //
                "http://www.apache.org/licenses/LICENSE-2.0");
    }

}
