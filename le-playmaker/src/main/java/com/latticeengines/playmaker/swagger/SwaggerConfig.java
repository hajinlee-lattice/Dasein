package com.latticeengines.playmaker.swagger;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import com.mangofactory.swagger.configuration.SpringSwaggerConfig;
import com.mangofactory.swagger.models.dto.ApiInfo;
import com.mangofactory.swagger.plugin.EnableSwagger;
import com.mangofactory.swagger.plugin.SwaggerSpringMvcPlugin;

@Configuration
@EnableSwagger
public class SwaggerConfig {
    private SpringSwaggerConfig springSwaggerConfig;

    @Autowired
    public void setSpringSwaggerConfig(SpringSwaggerConfig springSwaggerConfig) {
       this.springSwaggerConfig = springSwaggerConfig;
    }

    @Bean
    public SwaggerSpringMvcPlugin customImplementation(){
       return new SwaggerSpringMvcPlugin(springSwaggerConfig) //
               .apiInfo(apiInfo());
    }
    
    private ApiInfo apiInfo() {
        ApiInfo apiInfo = new ApiInfo(
                "Lattice Engines PlayMaker REST API",
                "This is the REST API exposed for the different PlayMaker services.",
                "termsofservice.html",
                "rgonzalez@lattice-engines.com",
                "License",
                "http://www.apache.org/licenses/LICENSE-2.0"
          );
        return apiInfo;
      }
    
}
