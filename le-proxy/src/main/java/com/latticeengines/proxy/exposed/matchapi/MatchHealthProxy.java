package com.latticeengines.proxy.exposed.matchapi;

import org.springframework.stereotype.Component;

import com.latticeengines.common.exposed.util.PropertyUtils;
import com.latticeengines.domain.exposed.StatusDocument;
import com.latticeengines.proxy.exposed.BaseRestApiProxy;

@Component("matchHealthProxy")
public class MatchHealthProxy extends BaseRestApiProxy {
    public MatchHealthProxy() {
        super(PropertyUtils.getProperty("common.matchapi.url"), "/match/health");
    }

    public StatusDocument dnbRateLimitStatus() {
        String url = constructUrl("/dnbstatus");
        return get("rateLimitStatus", url, StatusDocument.class);
    }
}
