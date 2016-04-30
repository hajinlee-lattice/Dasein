package com.latticeengines.oauth2.authserver;

import javax.annotation.Resource;
import javax.sql.DataSource;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.autoconfigure.velocity.VelocityAutoConfiguration;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.boot.context.web.SpringBootServletInitializer;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.EnableAspectJAutoProxy;
import org.springframework.context.annotation.ImportResource;
import org.springframework.security.oauth2.config.annotation.configurers.ClientDetailsServiceConfigurer;
import org.springframework.security.oauth2.config.annotation.web.configuration.AuthorizationServerConfigurerAdapter;
import org.springframework.security.oauth2.config.annotation.web.configuration.EnableAuthorizationServer;
import org.springframework.security.oauth2.config.annotation.web.configurers.AuthorizationServerEndpointsConfigurer;
import org.springframework.security.oauth2.config.annotation.web.configurers.AuthorizationServerSecurityConfigurer;
import org.springframework.security.oauth2.provider.token.AuthorizationServerTokenServices;
import org.springframework.security.oauth2.provider.token.store.JdbcTokenStore;

import com.latticeengines.monitor.exposed.metric.service.StatsService;
import com.latticeengines.oauth2.exception.ExceptionEncodingTranslator;

@Configuration
@EnableAutoConfiguration(exclude = {VelocityAutoConfiguration.class})
@EnableAspectJAutoProxy(proxyTargetClass = true)
@ImportResource(value = { "classpath:oauth2-authserver-context.xml", "classpath:oauth2-properties-context.xml" })
public class OAuthServer extends SpringBootServletInitializer {

    @Autowired
    private StatsService statsService;

    @Override
    protected SpringApplicationBuilder configure(SpringApplicationBuilder application) {
        return application.sources(OAuthServer.class);
    }

    public static void main(String[] args) {
        SpringApplication.run(OAuthServer.class, args);
    }

    @Configuration
    @EnableAuthorizationServer
    protected static class ServerConfig extends AuthorizationServerConfigurerAdapter {

        @Resource(name = "dataSourceOauth2")
        private DataSource dataSource;

        @Autowired
        private OneTimeKeyAuthenticationManager authenticationManager;

        @Bean
        public JdbcTokenStore tokenStore() {
            return new JdbcTokenStore(dataSource);
        }

        @Bean
        public AuthorizationServerTokenServices tokenServices() {
            JdbcTokenStore tokenStore = tokenStore();
            LatticeTokenServices tokenServices = new LatticeTokenServices(tokenStore);
            tokenServices.setTokenStore(tokenStore);
            tokenServices.setSupportRefreshToken(true);
            tokenServices.setRefreshTokenValiditySeconds(60 * 60 * 24 * 180); // 180
                                                                              // days
            return tokenServices;
        }

        @Override
        public void configure(ClientDetailsServiceConfigurer clients) throws Exception {
            clients.jdbc(dataSource);
        }

        @Override
        public void configure(AuthorizationServerEndpointsConfigurer endpoints) throws Exception {
            endpoints.tokenServices(tokenServices()).authenticationManager(authenticationManager);

            ExceptionEncodingTranslator translator = new ExceptionEncodingTranslator();
            endpoints.exceptionTranslator(translator);
        }

        @Override
        public void configure(AuthorizationServerSecurityConfigurer oauthServer) throws Exception {
        }
    }

}