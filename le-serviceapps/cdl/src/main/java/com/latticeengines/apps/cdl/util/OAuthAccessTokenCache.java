package com.latticeengines.apps.cdl.util;

import static com.latticeengines.proxy.exposed.ProxyUtils.shortenCustomerSpace;

import java.util.Arrays;

import javax.annotation.PostConstruct;
import javax.inject.Inject;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.cache.CacheManager;
import org.springframework.cache.annotation.CachePut;
import org.springframework.cache.annotation.Cacheable;
import org.springframework.cache.support.CompositeCacheManager;
import org.springframework.stereotype.Component;

import com.latticeengines.cache.exposed.cachemanager.LocalCacheManager;
import com.latticeengines.domain.exposed.cache.CacheName;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.oauth.OauthClientType;
import com.latticeengines.proxy.exposed.oauth2.Oauth2RestApiProxy;

@Component("oAuthAccessTokenCache")
public class OAuthAccessTokenCache {
    private static final Logger log = LoggerFactory.getLogger(OAuthAccessTokenCache.class);

    @Inject
    private Oauth2RestApiProxy oauth2RestApiProxy;

    @Inject
    private CacheManager cacheManager;

    private LocalCacheManager<String, String> oAuthTokenCache;
    private final String oAuth2DanteAppId = "lattice.web.dante";

    @PostConstruct
    public void addCacheManager() {
        oAuthTokenCache = new LocalCacheManager<>(CacheName.DantePreviewTokenCache, str -> {
            String[] tokens = str.split("\\|");
            return getOauthTokenForTenant(shortenCustomerSpace(tokens[0]));
        }, 2000); //
        if (cacheManager instanceof CompositeCacheManager) {
            log.info("adding local entity cache manager to composite cache manager");
            ((CompositeCacheManager) cacheManager).setCacheManagers(Arrays.asList(oAuthTokenCache));
        }
    }

    @Cacheable(cacheNames = CacheName.Constants.DantePreviewTokenCacheName, key = "T(java.lang.String).format(\"%s|oAuthToken\", T(com.latticeengines.proxy.exposed.ProxyUtils).shortenCustomerSpace(#customerSpace))")
    public String getOauthTokenFromCache(String customerSpace) {
        log.info("Generating a new talking points preview OAuth token TenantId: " + customerSpace);
        return getOauthTokenForTenant(shortenCustomerSpace(customerSpace));
    }

    @CachePut(cacheNames = CacheName.Constants.DantePreviewTokenCacheName, key = "T(java.lang.String).format(\"%s|oAuthToken\", T(com.latticeengines.proxy.exposed.ProxyUtils).shortenCustomerSpace(#customerSpace))")
    public String refreshOauthTokenInCache(String customerSpace) {
        log.info("Refreshing the talking points preview OAuth token for TenantId: " + customerSpace);
        return getOauthTokenForTenant(shortenCustomerSpace(customerSpace));
    }

    private String getOauthTokenForTenant(String customerSpace) {
        return oauth2RestApiProxy.createOAuth2AccessToken(CustomerSpace.parse(customerSpace).toString(),
                oAuth2DanteAppId, OauthClientType.PLAYMAKER).getValue();
    }

    public boolean isInvalidToken(String customerSpace, String token) {
        return !oauth2RestApiProxy.isValidOauthToken(customerSpace, token);
    }
}
