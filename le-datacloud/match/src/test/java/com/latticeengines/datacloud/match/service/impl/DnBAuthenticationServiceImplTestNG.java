package com.latticeengines.datacloud.match.service.impl;

import static com.latticeengines.datacloud.match.service.impl.DnBAuthenticationServiceImpl.DNB_KEY_PREFIX;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import javax.inject.Inject;

import org.springframework.data.redis.core.RedisTemplate;
import org.testng.Assert;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import com.latticeengines.common.exposed.util.SleepUtils;
import com.latticeengines.common.exposed.util.ThreadPoolUtils;
import com.latticeengines.datacloud.match.service.impl.DnBAuthenticationServiceImpl.DnBTokenCache;
import com.latticeengines.datacloud.match.testframework.DataCloudMatchFunctionalTestNGBase;
import com.latticeengines.domain.exposed.datacloud.dnb.DnBKeyType;

public class DnBAuthenticationServiceImplTestNG extends DataCloudMatchFunctionalTestNGBase {

    @Inject
    private DnBAuthenticationServiceImpl dnBAuthenticationService;

    @Inject
    private RedisTemplate<String, Object> redisTemplate;

    @Test(groups = "functional", dataProvider = "keyTypes")
    public void testAuthentication(DnBKeyType keyType) {
        // test case 1: local cached token
        String token1 = dnBAuthenticationService.requestToken(keyType, null);
        String token2 = dnBAuthenticationService.requestToken(keyType, null);
        Assert.assertNotNull(token1);
        Assert.assertNotNull(token2);
        Assert.assertEquals(token1, token2);

        // test case 2: multiple threads report same expired token around same
        // time, they are expected to get same updated token
        Callable<String> task = () -> dnBAuthenticationService.requestToken(keyType, token1);
        ExecutorService executor = ThreadPoolUtils.getFixedSizeThreadPool(this.getClass().getSimpleName(), 5);
        List<Future<String>> futures = new ArrayList<>();
        for (int i = 0; i < 5; i++) {
            futures.add(executor.submit(task));
        }
        Set<String> tokens = new HashSet<>();
        for (Future<String> future : futures) {
            try {
                tokens.add(future.get(15, TimeUnit.SECONDS));
            } catch (InterruptedException | ExecutionException | TimeoutException e) {
                throw new RuntimeException(e);
            }
        }
        Assert.assertEquals(tokens.size(), 1);
        String token3 = tokens.iterator().next();
        Assert.assertNotEquals(token3, token1);

        // test case 3: redis cached token is expired
        redisTemplate.opsForValue().set(DNB_KEY_PREFIX + keyType,
                new DnBAuthenticationServiceImpl.DnBTokenCache(token3, 0));
        // trigger local cache refresh to request new token
        dnBAuthenticationService.refreshToken(keyType, null);
        DnBTokenCache redisCache = (DnBTokenCache) redisTemplate.opsForValue().get(DNB_KEY_PREFIX + keyType);
        // verify redis cached token is updated
        Assert.assertNotEquals(redisCache.getToken(), token3);
        // verify local cached token is same as redis cached token
        // Wait and make sure local cache has finished reloading
        SleepUtils.sleep(5000L);
        Assert.assertEquals(dnBAuthenticationService.requestToken(keyType, null), redisCache.getToken());
    }

    @DataProvider(name = "keyTypes")
    public Object[][] provideKeyTypes() {
        return new Object[][] { { DnBKeyType.REALTIME }, { DnBKeyType.DPLUS } };
    }

}
