package com.latticeengines.marketoadapter;

import java.util.List;

import javax.annotation.PostConstruct;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;
import org.springframework.http.converter.HttpMessageConverter;
import org.springframework.http.converter.StringHttpMessageConverter;
import org.springframework.http.converter.json.MappingJackson2HttpMessageConverter;
import org.springframework.web.servlet.config.annotation.EnableWebMvc;
import org.springframework.web.servlet.config.annotation.InterceptorRegistry;
import org.springframework.web.servlet.config.annotation.WebMvcConfigurerAdapter;

import com.latticeengines.camille.exposed.CamilleConfiguration;
import com.latticeengines.camille.exposed.CamilleEnvironment;
import com.latticeengines.camille.exposed.CamilleEnvironment.Mode;
import com.latticeengines.common.exposed.rest.RequestLogInterceptor;

@Configuration
@EnableWebMvc
@ComponentScan(basePackages = { "com.latticeengines.marketoadapter", "com.latticeengines.common.exposed.rest" })
public class MarketoAdapterConfiguration extends WebMvcConfigurerAdapter {
    @PostConstruct
    public void initialize() throws Exception {
        // Initialize Camille.
        CamilleConfiguration config = new CamilleConfiguration(properties.getPod(), properties.getZooKeeperAddress());

        CamilleEnvironment.start(Mode.RUNTIME, config);
        MarketoAdapterBootstrapper.register();
    }

    @Override
    public void addInterceptors(InterceptorRegistry registry) {
        registry.addInterceptor(interceptor);
    }

    @Override
    public void configureMessageConverters(List<HttpMessageConverter<?>> converters) {
        MappingJackson2HttpMessageConverter converter = new MappingJackson2HttpMessageConverter();
        converter.setPrettyPrint(true);

        converters.add(converter);
        converters.add(new StringHttpMessageConverter());
    }

    @Autowired
    private RequestLogInterceptor interceptor;

    @Autowired
    private MarketoAdapterProperties properties;
}
