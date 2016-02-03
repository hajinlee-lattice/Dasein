package com.latticeengines.scoringapi.infrastructure;

import java.util.List;
import java.util.Properties;

import javax.annotation.PostConstruct;

import org.python.util.PythonInterpreter;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;
import org.springframework.http.converter.HttpMessageConverter;
import org.springframework.http.converter.json.MappingJackson2HttpMessageConverter;
import org.springframework.web.servlet.config.annotation.InterceptorRegistry;
import org.springframework.web.servlet.config.annotation.WebMvcConfigurerAdapter;

import com.latticeengines.common.exposed.rest.RequestLogInterceptor;
import com.latticeengines.scoringapi.history.ScoreHistorian;

@Configuration
//@EnableWebMvc
@ComponentScan(basePackages = { "com.latticeengines.scoringapi", "com.latticeengines.common.exposed.rest" })
public class ScoringConfiguration extends WebMvcConfigurerAdapter {
    @PostConstruct
    public void initialize() throws Exception {
        // Initialize the python interpreter.
        Properties overrides = new Properties(System.getProperties());
        overrides.setProperty("python.cachedir.skip", "true");
        PythonInterpreter.initialize(System.getProperties(), overrides, new String[0]);

        // Initialize Camille.
//        CamilleConfiguration config = new CamilleConfiguration(properties.getPod(), properties.getZooKeeperAddress());
  //      CamilleEnvironment.start(Mode.RUNTIME, config);
    //    ScoringCamilleBootstrapper.register();
    }

    @Override
    public void addInterceptors(InterceptorRegistry registry) {
        registry.addInterceptor(interceptor);
        registry.addInterceptor(scoreHistorian);
    }

    @Override
    public void configureMessageConverters(List<HttpMessageConverter<?>> converters) {
        MappingJackson2HttpMessageConverter converter = new MappingJackson2HttpMessageConverter();
        converter.setPrettyPrint(true);

        converters.add(converter);
    }

    @Autowired
    private RequestLogInterceptor interceptor;

    @Autowired
    private ScoreHistorian scoreHistorian;

    @Autowired
    private ScoringProperties properties;
}
