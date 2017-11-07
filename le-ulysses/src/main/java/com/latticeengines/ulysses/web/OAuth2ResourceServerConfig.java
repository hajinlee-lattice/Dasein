package com.latticeengines.ulysses.web;

import java.util.Collections;
import java.util.List;

import javax.annotation.Resource;
import javax.sql.DataSource;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.security.config.annotation.web.builders.HttpSecurity;
import org.springframework.security.config.annotation.web.configuration.EnableWebSecurity;
import org.springframework.security.oauth2.config.annotation.web.configuration.ResourceServerConfiguration;
import org.springframework.security.oauth2.config.annotation.web.configuration.ResourceServerConfigurer;
import org.springframework.security.oauth2.config.annotation.web.configuration.ResourceServerConfigurerAdapter;
import org.springframework.security.oauth2.config.annotation.web.configurers.ResourceServerSecurityConfigurer;
import org.springframework.security.oauth2.provider.error.OAuth2AuthenticationEntryPoint;
import org.springframework.security.oauth2.provider.token.store.JdbcTokenStore;

import com.latticeengines.security.exposed.util.MultiTenantContext;

@Configuration
// @EnableResourceServer
@EnableWebSecurity
public class OAuth2ResourceServerConfig extends ResourceServerConfigurerAdapter {

    private static final String LP_REST_RESOURCE_ID = "lp_api";
    private static final String PLAYMAKER_REST_RESOURCE_ID = "playmaker_api";

    @Resource(name = "dataSourceOauth2")
    private DataSource dataSource;

    @Bean
    public JdbcTokenStore tokenStore() {
        return new JdbcTokenStore(dataSource);
    }

    @Bean
    protected ResourceServerConfiguration playmakerResources() {
        ResourceServerConfiguration resource = new ResourceServerConfiguration() {
            public void setConfigurers(List<ResourceServerConfigurer> configurers) {
                super.setConfigurers(configurers);
            }
        };
        resource.setConfigurers(
                Collections.<ResourceServerConfigurer> singletonList(new ResourceServerConfigurerAdapter() {
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
                        MultiTenantContext.setStrategy(new OAuth2MultiTenantContextStrategy());
                        // define URL patterns to enable OAuth2 security

                        // @formatter:off
                        http.authorizeRequests() //
                                .antMatchers( //
                                        "/ulysses/v2/api-docs", //
                                        "/ulysses/webjars/**", //
                                        "/**/favicon.ico", //
                                        "/ulysses/swagger-ui.html", //
                                        "/ulysses/swagger-resources/**", //
                                        "/ulysses/health/**") //
                                .permitAll() //
                                .antMatchers("/ulysses/latticeinsights/**", //
                                        "/ulysses/companyprofiles/**", //
                                        "/ulysses/datacollection/attributes/**", //
                                        "/ulysses/datacollection/accounts/**", //
                                        "/ulysses/recommendations/**", //
                                        "/ulysses/talkingpoints/**") //
                                .access("#oauth2.hasScope('read') or (!#oauth2.isOAuth() and hasRole('PLAYMAKER_CLIENT'))")
                                .antMatchers("/ulysses/**").denyAll();

                        // @formatter:on
                    }
                }));
        resource.setOrder(3);
        return resource;
    }

    @Bean
    protected ResourceServerConfiguration lpResources() {
        ResourceServerConfiguration resource = new ResourceServerConfiguration() {
            public void setConfigurers(List<ResourceServerConfigurer> configurers) {
                super.setConfigurers(configurers);
            }
        };
        resource.setConfigurers(
                Collections.<ResourceServerConfigurer> singletonList(new ResourceServerConfigurerAdapter() {
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
                        MultiTenantContext.setStrategy(new OAuth2MultiTenantContextStrategy());
                        // define URL patterns to enable OAuth2 security

                        // @formatter:off
                        http.authorizeRequests() //
                                .antMatchers( //
                                        "/ulysses/v2/api-docs", //
                                        "/ulysses/webjars/**", //
                                        "/**/favicon.ico", //
                                        "/ulysses/swagger-ui.html", //
                                        "/ulysses/swagger-resources/**", //
                                        "/ulysses/health/**") //
                                .permitAll() //
                                .antMatchers("/ulysses/latticeinsights/**", //
                                        "/ulysses/companyprofiles/**", //
                                        "/ulysses/attributes/**", "/ulysses/tenant/**") //
                                .access("#oauth2.hasScope('read') or (!#oauth2.isOAuth() and hasRole('LP_CLIENT'))")
                                .antMatchers("/ulysses/**").denyAll();

                        // @formatter:on
                    }
                }));
        resource.setOrder(4);
        return resource;
    }

    // @Override
    // public void configure(ResourceServerSecurityConfigurer resources) {
    // resources.tokenStore(tokenStore()).resourceId(LP_REST_RESOURCE_ID).stateless(false);
    // OAuth2AuthenticationEntryPoint authenticationEntryPoint = new
    // OAuth2AuthenticationEntryPoint();
    //
    // ExceptionEncodingTranslator translator = new
    // ExceptionEncodingTranslator();
    // authenticationEntryPoint.setExceptionTranslator(translator);
    // resources.authenticationEntryPoint(authenticationEntryPoint);
    // }
    //
    // @Override
    // public void configure(HttpSecurity http) throws Exception {
    // MultiTenantContext.setStrategy(new OAuth2MultiTenantContextStrategy());
    // // define URL patterns to enable OAuth2 security
    //
    // // @formatter:off
    // http.authorizeRequests() //
    // .antMatchers( //
    // "/ulysses/v2/api-docs", //
    // "/ulysses/webjars/**", //
    // "/**/favicon.ico", //
    // "/ulysses/swagger-ui.html", //
    // "/ulysses/swagger-resources/**", //
    // "/ulysses/health/**") //
    // .permitAll() //
    // .antMatchers("/ulysses/latticeinsights/**", //
    // "/ulysses/companyprofiles/**", //
    // "/ulysses/attributes/**", "/ulysses/tenant/**") //
    // .access("#oauth2.hasScope('read') or (!#oauth2.isOAuth() and
    // hasRole('LP_CLIENT'))")
    // .antMatchers("/ulysses/**").denyAll();
    //
    // // @formatter:on
    // }
}