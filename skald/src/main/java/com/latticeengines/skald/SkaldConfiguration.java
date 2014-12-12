package com.latticeengines.skald;

import java.util.List;

import javax.annotation.PostConstruct;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
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
public class SkaldConfiguration extends WebMvcConfigurerAdapter {
    @PostConstruct
    public void initialize() throws Exception {
        CamilleConfiguration config = new CamilleConfiguration(properties.getPod(), properties.getZooKeeperAddress());

        // TODO Swap this to runtime mode once a provisioning tool exists.
        CamilleEnvironment.start(Mode.BOOTSTRAP, config);
        SkaldBootstrapper.register();
    }

    @Override
    public void addInterceptors(InterceptorRegistry registry) {
        registry.addInterceptor(interceptor);
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
    private SkaldInterceptor interceptor;

    @Autowired
    private SkaldProperties properties;
}
