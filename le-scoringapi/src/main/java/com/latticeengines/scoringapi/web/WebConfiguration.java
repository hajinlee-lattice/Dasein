package com.latticeengines.scoringapi.web;

import java.util.List;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;
import org.springframework.http.converter.HttpMessageConverter;
import org.springframework.http.converter.json.MappingJackson2HttpMessageConverter;
import org.springframework.web.servlet.config.annotation.EnableWebMvc;
import org.springframework.web.servlet.config.annotation.ResourceHandlerRegistry;
import org.springframework.web.servlet.config.annotation.WebMvcConfigurerAdapter;

import com.latticeengines.common.exposed.rest.RequestLogInterceptor;

@Configuration
@EnableWebMvc
@ComponentScan(basePackages = { "com.latticeengines.db", "com.latticeengines.oauth2db", "com.latticeengines.scoringapi",
        "com.latticeengines.common.exposed.rest" })
public class WebConfiguration extends WebMvcConfigurerAdapter {

    @Autowired
    private RequestLogInterceptor requestLogInterceptor;

    public WebConfiguration() {
        super();
    }

    @Override
    public void configureMessageConverters(List<HttpMessageConverter<?>> converters) {
        MappingJackson2HttpMessageConverter converter = new MappingJackson2HttpMessageConverter();
        converter.setPrettyPrint(true);

        converters.add(converter);
    }

    @Override
    public void addResourceHandlers(final ResourceHandlerRegistry registry) {
        registry.addResourceHandler("swagger-ui.html").addResourceLocations("classpath:/META-INF/resources/");
        registry.addResourceHandler("/webjars/**").addResourceLocations("classpath:/META-INF/resources/webjars/");
    }

}
