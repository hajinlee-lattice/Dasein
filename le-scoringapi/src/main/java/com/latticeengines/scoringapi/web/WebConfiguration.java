package com.latticeengines.scoringapi.web;

import java.util.List;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.ImportResource;
import org.springframework.http.converter.HttpMessageConverter;
import org.springframework.http.converter.json.MappingJackson2HttpMessageConverter;
import org.springframework.web.servlet.config.annotation.EnableWebMvc;
import org.springframework.web.servlet.config.annotation.ResourceHandlerRegistry;
import org.springframework.web.servlet.config.annotation.WebMvcConfigurerAdapter;

import com.latticeengines.common.exposed.rest.RequestLogInterceptor;
import com.latticeengines.monitor.exposed.metric.service.StatsService;
import com.latticeengines.monitor.exposed.metric.stats.Inspection;
import com.latticeengines.monitor.exposed.metric.stats.impl.HealthInsepection;

@Configuration
@EnableWebMvc
@ComponentScan(basePackages = { "com.latticeengines.db", "com.latticeengines.oauth2db", "com.latticeengines.scoringapi",
        "com.latticeengines.common.exposed.rest" })
@ImportResource("classpath:monitor-metric-context.xml")
public class WebConfiguration extends WebMvcConfigurerAdapter {

    @Autowired
    private RequestLogInterceptor requestLogInterceptor;

    @Autowired
    private StatsService statsService;

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

    @Bean
    public Inspection healthCheck() {
        HealthInsepection healthInsepection = new HealthInsepection();
        healthInsepection.setStatsService(statsService);
        healthInsepection.setComponentName("scoringapi");
        return healthInsepection;
    }

}
