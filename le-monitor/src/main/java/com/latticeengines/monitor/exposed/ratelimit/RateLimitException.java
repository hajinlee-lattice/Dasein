package com.latticeengines.monitor.exposed.ratelimit;

import com.latticeengines.domain.exposed.exception.LedpCode;
import com.latticeengines.domain.exposed.exception.LedpException;

public class RateLimitException extends LedpException {
    private static final long serialVersionUID = 2716614122174152248L;
    
    private String url;

    public RateLimitException(LedpCode code, String[] params, String url) {
        super(code, params);
        this.setUrl(url);
    }

    public String getUrl() {
        return url;
    }

    public void setUrl(String url) {
        this.url = url;
    }
}
