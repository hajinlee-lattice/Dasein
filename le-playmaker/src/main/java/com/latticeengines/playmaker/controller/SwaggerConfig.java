package com.latticeengines.playmaker.controller;

import com.mangofactory.swagger.configuration.SpringSwaggerConfig;
import com.mangofactory.swagger.models.dto.ApiInfo;
import com.mangofactory.swagger.plugin.*;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.autoconfigure.velocity.VelocityAutoConfiguration;
import org.springframework.context.annotation.*;

@Configuration
@EnableSwagger
@EnableAutoConfiguration(exclude = {VelocityAutoConfiguration.class})
public class SwaggerConfig {

    private SpringSwaggerConfig springSwaggerConfig;

    @Autowired
    public void setSpringSwaggerConfig(SpringSwaggerConfig springSwaggerConfig) {
        this.springSwaggerConfig = springSwaggerConfig;
    }

    @Bean
    public SwaggerSpringMvcPlugin customImplementation() {
        return new SwaggerSpringMvcPlugin(this.springSwaggerConfig)
                // This info will be used in Swagger. See realisation of ApiInfo
                // for more details.
                .apiInfo(
                        new ApiInfo("Lattice PlayMaker Sync APIs", "This is for Lattice customers only.", null, null,
                                null, null))
                // Here we disable auto generating of responses for
                // REST-endpoints
                .useDefaultResponseMessages(false)
                // Here we specify URI patterns which will be included in
                // Swagger docs. Use regex for this purpose.
                .includePatterns("/playmaker.*");
    }

}
