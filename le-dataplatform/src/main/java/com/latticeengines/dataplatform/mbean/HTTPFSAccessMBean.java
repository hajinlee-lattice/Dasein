package com.latticeengines.dataplatform.mbean;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.jmx.export.annotation.ManagedOperation;
import org.springframework.jmx.export.annotation.ManagedResource;
import org.springframework.stereotype.Component;

import com.latticeengines.common.exposed.util.HttpUtils;

@Component("httpFSMBean")
@ManagedResource(objectName = "Diagnostics:name=HttpFSCheck")
public class HTTPFSAccessMBean {

    @Value("${dataplatform.fs.web.defaultFS}")
    private String webHDFS;

    @ManagedOperation(description = "Check HttpFS Accessibility")
    public String checkHttpAccess() {
        try {
            String url = webHDFS + "/app/dataplatform/dataplatform.properties?user.name=yarn&op=GETFILESTATUS";
            return "/app/dataplatform/dataplatform.properties: \n" + HttpUtils.executeGetRequest(url);
        } catch (Exception e) {
            return "Failed to access /app/dataplatform/dataplatform.properties from HttpFS due to: " + e.getMessage();
        }
    }
}
