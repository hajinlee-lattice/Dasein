package com.latticeengines.playmaker.controller;

import javax.annotation.Resource;
import javax.sql.DataSource;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.http.HttpMethod;
import org.springframework.security.config.annotation.web.builders.HttpSecurity;
import org.springframework.security.config.annotation.web.configuration.EnableWebSecurity;
import org.springframework.security.oauth2.config.annotation.web.configuration.EnableResourceServer;
import org.springframework.security.oauth2.config.annotation.web.configuration.ResourceServerConfigurerAdapter;
import org.springframework.security.oauth2.config.annotation.web.configurers.ResourceServerSecurityConfigurer;
import org.springframework.security.oauth2.provider.error.OAuth2AuthenticationEntryPoint;
import org.springframework.security.oauth2.provider.token.store.JdbcTokenStore;

import com.latticeengines.oauth2db.exposed.tokenstore.JsonJdbcTokenStore;
import com.latticeengines.playmaker.exception.ExceptionEncodingTranslator;

@Configuration
@EnableResourceServer
@EnableWebSecurity
public class OAuth2ResourceServerConfig extends ResourceServerConfigurerAdapter {

    private static final String PLAYMAKER_REST_RESOURCE_ID = "playmaker_api";

    @Resource(name = "dataSourceOauth2")
    private DataSource dataSource;

    @Bean
    public JdbcTokenStore tokenStore() {
        return new JsonJdbcTokenStore(dataSource);
    }

    @Override
    public void configure(ResourceServerSecurityConfigurer resources) {
        resources.tokenStore(tokenStore()).resourceId(PLAYMAKER_REST_RESOURCE_ID).stateless(false);
        OAuth2AuthenticationEntryPoint authenticationEntryPoint = new OAuth2AuthenticationEntryPoint();

        ExceptionEncodingTranslator translator = new ExceptionEncodingTranslator();
        authenticationEntryPoint.setExceptionTranslator(translator);
        resources.authenticationEntryPoint(authenticationEntryPoint);
    }

    @Override
    public void configure(HttpSecurity http) throws Exception {

        // define URL patterns to enable OAuth2 security

        http.requestMatchers()
                .antMatchers("/playmaker/**", //
                        "/tenants/**", //
                        "/v2/api-docs", //
                        "/swagger-ui.html", //
                        "/webjars/**", //
                        "/**/favicon.ico", //
                        "/swagger-resources", //
                        "/configuration/**", //
                        "/playmaker/v2/api-docs", //
                        "/playmaker/swagger-ui.html", //
                        "/playmaker/webjars/**", //
                        "/playmaker/swagger-resources", //
                        "/playmaker/configuration/**")
                .and().authorizeRequests() //
                .antMatchers("/playmaker/health", //
                        "/health", //
                        "/v2/api-docs", //
                        "/swagger-ui.html", //
                        "/webjars/**", //
                        "/**/favicon.ico", //
                        "/swagger-resources", //
                        "/configuration/**", //
                        "/playmaker/v2/api-docs", //
                        "/playmaker/swagger-ui.html", //
                        "/playmaker/webjars/**", //
                        "/playmaker/**/favicon.ico", //
                        "/playmaker/swagger-resources", //
                        "/playmaker/configuration/**")
                .permitAll()
                .antMatchers(HttpMethod.POST, "/tenants", "/playmaker/tenants").permitAll() //
                .antMatchers("/tenants/**", "/playmaker/tenants/**")
                .access("#oauth2.hasScope('write') or (!#oauth2.isOAuth() and hasRole('PLAYMAKER_ADMIN'))") //
                .antMatchers("/playmaker/**") //
                .access("#oauth2.hasScope('read') or (!#oauth2.isOAuth() and hasRole('PLAYMAKER_CLIENT'))");
    }

}