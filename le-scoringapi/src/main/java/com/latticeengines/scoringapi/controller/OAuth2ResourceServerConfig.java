package com.latticeengines.scoringapi.controller;

import javax.annotation.Resource;
import javax.sql.DataSource;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.security.config.annotation.web.builders.HttpSecurity;
import org.springframework.security.config.annotation.web.configuration.EnableWebSecurity;
import org.springframework.security.oauth2.config.annotation.web.configuration.EnableResourceServer;
import org.springframework.security.oauth2.config.annotation.web.configuration.ResourceServerConfigurerAdapter;
import org.springframework.security.oauth2.config.annotation.web.configurers.ResourceServerSecurityConfigurer;
import org.springframework.security.oauth2.provider.error.OAuth2AuthenticationEntryPoint;
import org.springframework.security.oauth2.provider.token.store.JdbcTokenStore;

import com.latticeengines.scoringapi.exposed.exception.ExceptionEncodingTranslator;

@Configuration
@EnableResourceServer
@EnableWebSecurity
public class OAuth2ResourceServerConfig extends ResourceServerConfigurerAdapter {

    private static final String LP_REST_RESOURCE_ID = "lp_api";

    @Resource(name = "dataSourceOauth2")
    private DataSource dataSource;

    @Bean
    public JdbcTokenStore tokenStore() {
        return new JdbcTokenStore(dataSource);
    }

    @Override
    public void configure(ResourceServerSecurityConfigurer resources) {
        resources.tokenStore(tokenStore()).resourceId(LP_REST_RESOURCE_ID).stateless(false);
        OAuth2AuthenticationEntryPoint authenticationEntryPoint = new OAuth2AuthenticationEntryPoint();

        ExceptionEncodingTranslator translator = new ExceptionEncodingTranslator();
        authenticationEntryPoint.setExceptionTranslator(translator);
        resources.authenticationEntryPoint(authenticationEntryPoint);
    }

    @Override
    public void configure(HttpSecurity http) throws Exception {
        // define URL patterns to enable OAuth2 security

        // @formatter:off
        http.requestMatchers().
                antMatchers("/score/**", "/api-docs/**", "/swagger/**").and().
                authorizeRequests().
                    antMatchers("/score/configuration/**", "/score/webjars/**", "/score/v2/**", "/score/api-docs", "/score/api-docs/**", "/score/swagger/**", "/score/swagger**", "/score/health/**").permitAll().
                    antMatchers("/score/**").access("#oauth2.hasScope('read') or (!#oauth2.isOAuth() and hasRole('LP_CLIENT'))");
        // @formatter:on
    }

}