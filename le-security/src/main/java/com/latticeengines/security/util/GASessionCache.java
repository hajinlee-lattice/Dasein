package com.latticeengines.security.util;

import java.util.List;
import java.util.Random;
import java.util.concurrent.TimeUnit;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.latticeengines.domain.exposed.exception.LedpCode;
import com.latticeengines.domain.exposed.exception.LedpException;
import com.latticeengines.domain.exposed.security.Session;
import com.latticeengines.domain.exposed.security.Ticket;
import com.latticeengines.security.exposed.AccessLevel;
import com.latticeengines.security.exposed.GrantedRight;
import com.latticeengines.security.exposed.globalauth.GlobalSessionManagementService;

public class GASessionCache {

    private static Log log = LogFactory.getLog(GASessionCache.class);
    private static final Integer MAX_RETRY = 3;
    private static Long retryIntervalMsec = 200L;
    private static Random random = new Random(System.currentTimeMillis());
    private LoadingCache<String, Session> tokenExpirationCache;

    public GASessionCache(final GlobalSessionManagementService globalSessionMgr, int cacheExpiration) {
        tokenExpirationCache = CacheBuilder.newBuilder().maximumSize(1000)
                .expireAfterWrite(cacheExpiration, TimeUnit.SECONDS).build(new CacheLoader<String, Session>() {
                    @Override
                    public Session load(String token) throws Exception {
                        try {
                            log.info("Loading session from GA to cache for token " + token);
                            Ticket ticket = new Ticket(token);
                            Long retryInterval = retryIntervalMsec;
                            Integer retries = 0;
                            Session session = null;
                            while (++retries <= MAX_RETRY) {
                                try {
                                    session = globalSessionMgr.retrieve(ticket);
                                } catch (Exception e) {
                                    log.warn("Failed to retrieve session " + token + " from GA - retried " + retries
                                            + " out of " + MAX_RETRY + " times", e);
                                } finally {
                                    try {
                                        retryInterval = new Double(retryInterval * (1 + 1.0 * random.nextInt(1000) / 1000)).longValue();
                                        Thread.sleep(retryInterval);
                                    } catch (Exception e) {
                                        // ignore
                                    }
                                }
                            }
                            if (session != null && session.getRights() != null && !session.getRights().isEmpty()) {
                                interpretGARights(session);
                            }
                            return session;
                        } catch (Exception e) {
                            log.warn(String.format("Encountered an error when retrieving session %s from GA: "
                                    + e.getMessage() + " Invalidate the cache.", token), e);
                            return null;
                        }
                    }
                });
    }

    public Session retrieve(String token) {
        try {
            return tokenExpirationCache.get(token);
        } catch (Exception e) {
            throw new LedpException(LedpCode.LEDP_18002, e, new String[] { token });
        }
    }

    public void put(String token, Session session) {
        log.info("Putting a session into cache for token " + token);
        tokenExpirationCache.put(token, session);
    }

    public void removeToken(String token) {
        log.info("Removing token " + token + " from cache.");
        tokenExpirationCache.invalidate(token);
    }

    @VisibleForTesting
    void removeAll() {
        tokenExpirationCache.invalidateAll();
    }

    @VisibleForTesting
    void setRetryIntervalMsec(Long intervalMsec) {
        retryIntervalMsec = intervalMsec;
    }

    private static void interpretGARights(Session session) {
        List<String> GARights = session.getRights();
        try {
            AccessLevel level = AccessLevel.findAccessLevel(GARights);
            session.setRights(GrantedRight.getAuthorities(level.getGrantedRights()));
            session.setAccessLevel(level.name());
        } catch (NullPointerException e) {
            if (!GARights.isEmpty()) {
                AccessLevel level = isInternalEmail(session.getEmailAddress()) ? AccessLevel.INTERNAL_USER
                        : AccessLevel.EXTERNAL_USER;
                session.setRights(GrantedRight.getAuthorities(level.getGrantedRights()));
                session.setAccessLevel(level.name());
            }
            log.warn(String.format("Failed to interpret GA rights: %s for user %s in tenant %s. Use %s instead: %s",
                    GARights.toString(), session.getEmailAddress(), session.getTenant().getId(),
                    session.getAccessLevel(), e.getMessage()));
        }
    }

    private static boolean isInternalEmail(String email) {
        return email.toLowerCase().endsWith("lattice-engines.com");
    }

}
