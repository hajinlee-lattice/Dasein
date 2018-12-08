package com.latticeengines.microservice.swagger;

import javax.inject.Inject;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import com.latticeengines.microservice.exposed.DocketProvider;

import springfox.documentation.spring.web.plugins.Docket;
import springfox.documentation.swagger2.annotations.EnableSwagger2;

@Configuration
@EnableSwagger2
public class SwaggerConfig {

    @Inject
    private DocketProvider docketProvider;

    @Bean
    public Docket api() {
        return docketProvider.getDocket();
    }
}
