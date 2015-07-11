package com.latticeengines.playmaker.controller;

import javax.annotation.Resource;
import javax.sql.DataSource;

import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.http.HttpMethod;
import org.springframework.security.config.annotation.web.builders.HttpSecurity;
import org.springframework.security.oauth2.config.annotation.web.configuration.EnableResourceServer;
import org.springframework.security.oauth2.config.annotation.web.configuration.ResourceServerConfigurerAdapter;
import org.springframework.security.oauth2.config.annotation.web.configurers.ResourceServerSecurityConfigurer;
import org.springframework.security.oauth2.provider.token.store.JdbcTokenStore;

@Configuration
@EnableAutoConfiguration
@EnableResourceServer
public class OAuth2ResourceServerConfig extends ResourceServerConfigurerAdapter {

    private static final String PLAYMAKER_REST_RESOURCE_ID = "playmaker_api";

    @Resource(name = "dataSourceOauth2")
    private DataSource dataSource;

    @Bean
    public JdbcTokenStore tokenStore() {
        return new JdbcTokenStore(dataSource);
    }

    @Override
    public void configure(ResourceServerSecurityConfigurer resources) {
        resources.tokenStore(tokenStore()).resourceId(PLAYMAKER_REST_RESOURCE_ID).stateless(false);
    }

    @Override
    public void configure(HttpSecurity http) throws Exception {

        // define URL patterns to enable OAuth2 security

        http.requestMatchers().antMatchers("/playmaker/**", "/tenants/**", "/api-docs/**", "/swagger/**").and()
                .authorizeRequests().antMatchers("/playmaker/**")
                .access("#oauth2.hasScope('read') or (!#oauth2.isOAuth() and hasRole('PLAYMAKER_CLIENT'))")
                .antMatchers(HttpMethod.POST, "/tenants").permitAll().antMatchers("/tenants/**")
                .access("#oauth2.hasScope('write') or (!#oauth2.isOAuth() and hasRole('PLAYMAKER_ADMIN'))")
                .antMatchers("/api-docs", "/api-docs/**", "/swagger/**").permitAll();
    }
}