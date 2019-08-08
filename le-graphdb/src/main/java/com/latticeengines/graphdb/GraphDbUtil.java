package com.latticeengines.graphdb;

import javax.annotation.PostConstruct;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import com.latticeengines.domain.exposed.graph.NameSpaceUtil;

@Component
public class GraphDbUtil {

    @Value("${graphdb.ns.env}")
    private String defaultEnv;

    @Value("${graphdb.ns.version}")
    private String defaultVersion;

    @Value("${graphdb.ns.is.postfix:true}")
    private Boolean isNSPostfix;

    @Value("${graphdb.exception.ignore}")
    private Boolean ignoreException;

    private NameSpaceUtil nameSpaceUtil;

    @PostConstruct
    public void postConstruct() {
        nameSpaceUtil = new NameSpaceUtil(defaultEnv, defaultVersion);
    }

    public String getDefaultEnv() {
        return defaultEnv;
    }

    public void setDefaultEnv(String defaultEnv) {
        this.defaultEnv = defaultEnv;
    }

    public String getDefaultVersion() {
        return defaultVersion;
    }

    public void setDefaultVersion(String defaultVersion) {
        this.defaultVersion = defaultVersion;
    }

    public Boolean getIsNSPostfix() {
        return isNSPostfix;
    }

    public void setIsNSPostfix(Boolean isNSPostfix) {
        this.isNSPostfix = isNSPostfix;
    }

    public NameSpaceUtil getNameSpaceUtil() {
        return nameSpaceUtil;
    }

    public void setNameSpaceUtil(NameSpaceUtil nameSpaceUtil) {
        this.nameSpaceUtil = nameSpaceUtil;
    }

    public Boolean getIgnoreException() {
        return ignoreException;
    }

    public void setIgnoreException(Boolean ignoreException) {
        this.ignoreException = ignoreException;
    }
}
