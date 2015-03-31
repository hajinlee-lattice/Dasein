package com.latticeengines.marketoadapter;

import java.util.List;

import javax.annotation.PostConstruct;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;
import org.springframework.http.converter.HttpMessageConverter;
import org.springframework.http.converter.json.MappingJackson2HttpMessageConverter;
import org.springframework.web.servlet.config.annotation.EnableWebMvc;
import org.springframework.web.servlet.config.annotation.InterceptorRegistry;
import org.springframework.web.servlet.config.annotation.WebMvcConfigurerAdapter;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.latticeengines.camille.exposed.CamilleConfiguration;
import com.latticeengines.camille.exposed.CamilleEnvironment;
import com.latticeengines.camille.exposed.CamilleEnvironment.Mode;

@Configuration
@EnableWebMvc
@ComponentScan(basePackages = { "com.latticeengines.marketoadapter", "com.latticeengines.common.exposed.rest" })
public class MarketoAdapterConfiguration extends WebMvcConfigurerAdapter {
    @PostConstruct
    public void initialize() throws Exception {
        CamilleConfiguration config = new CamilleConfiguration(properties.getPod(), properties.getZooKeeperAddress());

        CamilleEnvironment.start(Mode.RUNTIME, config);
        MarketoAdapterBootstrapper.register();
    }

    @Override
    public void addInterceptors(InterceptorRegistry registry) {
    }

    @Override
    public void configureMessageConverters(List<HttpMessageConverter<?>> converters) {
        converters.add(converter());
    }

    @Bean
    MappingJackson2HttpMessageConverter converter() {
        MappingJackson2HttpMessageConverter converter = new MappingJackson2HttpMessageConverter();
        ObjectMapper mapper = new ObjectMapper();
        mapper.configure(SerializationFeature.INDENT_OUTPUT, true);
        converter.setObjectMapper(mapper);

        return converter;
    }

    @Autowired
    private MarketoAdapterProperties properties;
}
