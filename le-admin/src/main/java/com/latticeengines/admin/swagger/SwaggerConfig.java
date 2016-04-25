package com.latticeengines.admin.swagger;

import static com.google.common.collect.Lists.newArrayList;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.ImportResource;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.servlet.config.annotation.EnableWebMvc;

import springfox.documentation.builders.ApiInfoBuilder;
import springfox.documentation.builders.PathSelectors;
import springfox.documentation.builders.ResponseMessageBuilder;
import springfox.documentation.schema.ModelRef;
import springfox.documentation.service.ApiInfo;
import springfox.documentation.spi.DocumentationType;
import springfox.documentation.spring.web.plugins.Docket;
import springfox.documentation.swagger2.annotations.EnableSwagger2;

import com.latticeengines.common.exposed.util.SwaggerUtils;
import com.latticeengines.common.exposed.version.VersionManager;

@Configuration
@EnableWebMvc
@ImportResource("classpath:common-component-context.xml")
@EnableSwagger2
public class SwaggerConfig {

    @Autowired
    private VersionManager versionManager;

    @Bean
    public Docket api() {
        return new Docket(DocumentationType.SWAGGER_2) //
                .select() //
                .apis(SwaggerUtils.getApiSelector("com.latticeengines.admin.controller.*",
                        "com.latticeengines.security.controller.ActiveDirectoryLoginResource")) //
                .paths(PathSelectors.any()) //
                .build() //
                .pathMapping("/admin") //
                .apiInfo(apiInfo()) //
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
                .title("Lattice Engines Admin REST API") //
                .description("This is the REST API exposed for the different admin services.") //
                .version(versionManager.getCurrentVersion()) //
                .termsOfServiceUrl("termsofservice.html") //
                .license("License") //
                .licenseUrl("http://www.apache.org/licenses/LICENSE-2.0") //
                .build();
    }

}