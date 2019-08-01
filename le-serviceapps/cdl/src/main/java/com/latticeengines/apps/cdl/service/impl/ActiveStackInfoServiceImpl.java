package com.latticeengines.apps.cdl.service.impl;

import java.util.Map;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.cache.annotation.Cacheable;
import org.springframework.core.ParameterizedTypeReference;
import org.springframework.http.HttpMethod;
import org.springframework.http.ResponseEntity;
import org.springframework.retry.support.RetryTemplate;
import org.springframework.stereotype.Component;
import org.springframework.web.client.RestTemplate;

import com.latticeengines.apps.cdl.service.ActiveStackInfoService;
import com.latticeengines.common.exposed.util.HttpClientUtils;
import com.latticeengines.common.exposed.util.RetryUtils;
import com.latticeengines.domain.exposed.cache.CacheName;

@Component("activeStackInfoService")
public class ActiveStackInfoServiceImpl implements ActiveStackInfoService {

    // TODO move to a common place
    private static final String CURRENT_STACK_KEY = "CurrentStack";
    private static final String STACK_INFO_PATH = "/pls/health/stackinfo";

    private RestTemplate restTemplate = HttpClientUtils.newRestTemplate();

    private RetryTemplate retryTemplate = getRetryTemplate();

    @Value("${cdl.app.public.url:https://localhost:9081}")
    private String appPublicUrl;

    @Value("${common.le.stack}")
    private String leStack;

    @Override
    public boolean isCurrentStackActive() {
        return leStack.equalsIgnoreCase(getActiveStack());
    }

    @Override
    public String getActiveStack() {
        Map<String, String> stackInfo = getActiveStackInfo();
        return stackInfo == null ? null : stackInfo.get(CURRENT_STACK_KEY);
    }

    @Override
    @Cacheable(cacheNames = CacheName.Constants.ActiveStackInfoCacheName, key = "T(java.lang.String).format(\"%s|active_stack_info\", #root.target.getAppPublicUrl())", unless="#result == null")
    public Map<String, String> getActiveStackInfo() {
        String url = appPublicUrl + STACK_INFO_PATH;
        return retryTemplate.execute(ctx -> {
            ResponseEntity<Map<String, String>> res = restTemplate.exchange(url, HttpMethod.GET, null,
                    new ParameterizedTypeReference<Map<String, String>>() {});
            return res.getBody();
        });
    }

    // only use by cache
    public String getAppPublicUrl() {
        return appPublicUrl;
    }

    private RetryTemplate getRetryTemplate() {
        // TODO tune retry parameter later if needed
        return RetryUtils.getExponentialBackoffRetryTemplate(5, 2000L, 2D, 60000L, true, null);
    }
}
