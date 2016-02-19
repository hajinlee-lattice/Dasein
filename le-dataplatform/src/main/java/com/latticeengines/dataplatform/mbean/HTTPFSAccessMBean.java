package com.latticeengines.dataplatform.mbean;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.jmx.export.annotation.ManagedOperation;
import org.springframework.jmx.export.annotation.ManagedResource;
import org.springframework.stereotype.Component;

import com.latticeengines.common.exposed.util.HttpWithRetryUtils;
import com.latticeengines.common.exposed.version.VersionManager;

@Component("httpFSMBean")
@ManagedResource(objectName = "Diagnostics:name=HttpFSCheck")
public class HTTPFSAccessMBean {

    @Value("${dataplatform.fs.web.defaultFS}")
    private String webHDFS;

    @Autowired
    private VersionManager versionManager;

    @ManagedOperation(description = "Check HttpFS Accessibility")
    public String checkHttpAccess() {
        try {
            String s = versionManager.getCurrentVersion().equals("") ? "" : "/";
            String url = String.format("%s/app/%s%sdataplatform/dataplatform.properties?user.name=yarn&op=GETFILESTATUS", webHDFS, versionManager.getCurrentVersion(),s);
            return "dataplatform.properties: \n" + HttpWithRetryUtils.executeGetRequest(url);
        } catch (Exception e) {
            return "Failed to access dataplatform.properties from HttpFS due to: " + e.getMessage();
        }
    }
}
