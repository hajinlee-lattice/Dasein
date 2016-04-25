package com.latticeengines.microservice.swagger;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import springfox.documentation.spring.web.plugins.Docket;
import springfox.documentation.swagger2.annotations.EnableSwagger2;

import com.latticeengines.microservice.exposed.DocketProvider;

@Configuration
@EnableSwagger2
public class SwaggerConfig {

    @Autowired
    private DocketProvider docketProvider;

    @Bean
    public Docket api() {
        return docketProvider.getDocket();
    }
}
