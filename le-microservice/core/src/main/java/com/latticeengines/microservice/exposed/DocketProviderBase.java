package com.latticeengines.microservice.exposed;

import static com.google.common.collect.Lists.newArrayList;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.RequestMethod;

import springfox.documentation.RequestHandler;
import springfox.documentation.builders.ApiInfoBuilder;
import springfox.documentation.builders.PathSelectors;
import springfox.documentation.builders.ResponseMessageBuilder;
import springfox.documentation.schema.ModelRef;
import springfox.documentation.service.ApiInfo;
import springfox.documentation.spi.DocumentationType;
import springfox.documentation.spring.web.plugins.Docket;

import com.google.common.base.Predicate;
import com.latticeengines.common.exposed.version.VersionManager;

public abstract class DocketProviderBase implements DocketProvider {

    @Autowired
    private VersionManager apiVersionManager;

    public Docket getDocket() {
        return new Docket(DocumentationType.SWAGGER_2) //
                .select() //
                .apis(apiSelector()) //
                .paths(PathSelectors.any()) //
                .build() //
                .pathMapping(contextPath()) //
                .apiInfo(apiInfo())
                .useDefaultResponseMessages(false).globalResponseMessage(RequestMethod.GET,
                        newArrayList(
                                new ResponseMessageBuilder().code(500).message("500 message")
                                        .responseModel(new ModelRef("Error")).build(), //
                                new ResponseMessageBuilder().code(400).message("Bad Request").build(), //
                                new ResponseMessageBuilder().code(401).message("Unauthorized").build(), //
                                new ResponseMessageBuilder().code(402).message("Request Failed").build() //
                        ));
    }

    private ApiInfo apiInfo() {
        return new ApiInfoBuilder() //
                .title("Lattice Engines " + moduleName() + " REST API") //
                .description("This is the REST API exposed for the " + moduleName() + " services.") //
                .version(apiVersionManager.getCurrentVersion()) //
                .termsOfServiceUrl("termsofservice.html") //
                .license("License") //
                .licenseUrl("http://www.apache.org/licenses/LICENSE-2.0") //
                .build();
    }

    protected abstract String moduleName();
    protected abstract Predicate<RequestHandler> apiSelector();
    protected abstract String contextPath();

}
