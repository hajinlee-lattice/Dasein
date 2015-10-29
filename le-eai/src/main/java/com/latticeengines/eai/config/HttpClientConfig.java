package com.latticeengines.eai.config;

import com.latticeengines.common.exposed.util.JsonUtils;

public class HttpClientConfig {

    private int connectTimeout;

    private int importTimeout;

    public HttpClientConfig(int connectTimeout, int importTimeout) {
        this.connectTimeout = connectTimeout;
        this.importTimeout = importTimeout;
    }

    public int getConnectTimeout() {
        return this.connectTimeout;
    }

    public void setConnectTimeout(int connectTimeout) {
        this.connectTimeout = connectTimeout;
    }

    public int getImportTimeout() {
        return this.importTimeout;
    }

    public void setImportTimeout(int importTimeout) {
        this.importTimeout = importTimeout;
    }

    public String toString() {
        return JsonUtils.serialize(this);
    }
}
