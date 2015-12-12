package com.latticeengines.security.util;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.python.antlr.ast.Exec;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.latticeengines.domain.exposed.exception.LedpCode;
import com.latticeengines.domain.exposed.exception.LedpException;
import com.latticeengines.domain.exposed.security.Session;
import com.latticeengines.domain.exposed.security.Ticket;
import com.latticeengines.security.exposed.globalauth.GlobalSessionManagementService;

public class GASessionCache {

    private static Log log = LogFactory.getLog(GASessionCache.class);
    private LoadingCache<String, Session> tokenExpirationCache;

    public GASessionCache(final GlobalSessionManagementService globalSessionMgr, int cacheExpiration) {
        tokenExpirationCache = CacheBuilder.newBuilder()
                .maximumSize(1000)
                .expireAfterAccess(cacheExpiration, TimeUnit.SECONDS)
                .build(new CacheLoader<String, Session>() {
                    @Override
                    public Session load(String token) throws Exception {
                        try {
                            Ticket ticket = new Ticket(token);
                            return globalSessionMgr.retrieve(ticket);
                        } catch (Exception e) {
                            log.warn(String.format(
                                    "Encountered an error when retrieving session %s from GA. " +
                                            "Invalidate the cache.", token));
                            return null;
                        }
                    }
                });
    }

    public Session retrieve(String token) {
        try {
            return tokenExpirationCache.get(token);
        } catch (Exception e) {
            throw new LedpException(LedpCode.LEDP_18002, e, new String[]{ token });
        }
    }

    void removeAll() {
        tokenExpirationCache.invalidateAll();
    }

    public void removeToken(String token) {
        tokenExpirationCache.invalidate(token);
    }

}
