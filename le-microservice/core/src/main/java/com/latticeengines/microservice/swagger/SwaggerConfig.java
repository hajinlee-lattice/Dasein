package com.latticeengines.microservice.swagger;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import com.latticeengines.microservice.exposed.AppInfoProvider;
import com.mangofactory.swagger.configuration.SpringSwaggerConfig;
import com.mangofactory.swagger.models.dto.ApiInfo;
import com.mangofactory.swagger.plugin.EnableSwagger;
import com.mangofactory.swagger.plugin.SwaggerSpringMvcPlugin;


@Configuration
@EnableSwagger
public class SwaggerConfig {
    
    private SpringSwaggerConfig springSwaggerConfig;
    private AppInfoProvider appInfoProvider;

    @Autowired
    public void setSpringSwaggerConfig(SpringSwaggerConfig springSwaggerConfig) {
        this.springSwaggerConfig = springSwaggerConfig;
    }
    
    @Autowired
    public void setAppInfoProvider(AppInfoProvider appInfoProvider) {
        this.appInfoProvider = appInfoProvider;
    }

    @Bean
    public SwaggerSpringMvcPlugin customImplementation() {
        return new SwaggerSpringMvcPlugin(springSwaggerConfig).apiInfo(apiInfo());
    }

    private ApiInfo apiInfo() {
        return appInfoProvider.apiInfo();
    }

}
