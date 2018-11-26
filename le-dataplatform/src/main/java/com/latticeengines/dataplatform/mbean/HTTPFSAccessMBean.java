package com.latticeengines.dataplatform.mbean;

import javax.inject.Inject;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.jmx.export.annotation.ManagedOperation;
import org.springframework.jmx.export.annotation.ManagedResource;
import org.springframework.stereotype.Component;
import org.springframework.web.client.RestTemplate;

import com.latticeengines.common.exposed.util.HttpClientUtils;
import com.latticeengines.common.exposed.version.VersionManager;
import com.latticeengines.hadoop.exposed.service.EMRCacheService;

@Component("httpFSMBean")
@ManagedResource(objectName = "Diagnostics:name=HttpFSCheck")
public class HTTPFSAccessMBean {

    @Value("${hadoop.fs.web.defaultFS}")
    private String webHDFS;

    @Value("${hadoop.use.emr}")
    private Boolean useEmr;

    @Inject
    private EMRCacheService emrCacheService;

    @Inject
    private VersionManager versionManager;

    @Value("${dataplatform.hdfs.stack:}")
    private String stackName;

    @ManagedOperation(description = "Check HttpFS Accessibility")
    public String checkHttpAccess() {
        try {
            String s = versionManager.getCurrentVersionInStack(stackName).equals("") ? "" : "/";
            String url = String.format(
                    "%s/app/%s%sconf/latticeengines.properties?user.name=yarn&op=GETFILESTATUS", getWebHdfs(),
                    versionManager.getCurrentVersionInStack(stackName), s);
            RestTemplate restTemplate = HttpClientUtils.newRestTemplate();
            return "latticeengines.properties: \n" + restTemplate.getForObject(url, String.class);
        } catch (Exception e) {
            return "Failed to access latticeengines.properties from HttpFS due to: " + e.getMessage();
        }
    }

    private String getWebHdfs() {
        if (Boolean.TRUE.equals(useEmr)) {
            return emrCacheService.getWebHdfsUrl();
        } else {
            return webHDFS;
        }
    }
}
