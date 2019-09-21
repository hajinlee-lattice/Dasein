package com.latticeengines.dataplatform.mbean;

import java.util.ArrayList;
import java.util.List;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jmx.export.annotation.ManagedOperation;
import org.springframework.jmx.export.annotation.ManagedResource;
import org.springframework.stereotype.Component;

@Component("dataplatformCheckMBean")
@ManagedResource(objectName = "Diagnostics:name=DataplatformCheck")
public class DataplatformCheckMBean {
    
    @Autowired
    private DBConnectionMBean dbcMBean;
    
    @Autowired
    private HDFSAccessMBean hdfsAcMBean;
    
    @Autowired
    private HDFSResourceMBean hdfsRcMBean;
    
    @Autowired
    private HTTPFSAccessMBean httpFSMBean;

    @ManagedOperation(description = "Check Dataplatform")
    public String checkDataplatform() {
        StringBuilder sb = new StringBuilder();
        String prefix = "Dataplatform check failed due to: \n";
        sb.append(prefix);

        List<String> checkRes = new ArrayList<String>();
        checkRes.add(dbcMBean.checkLedpConnection());
        // checkRes.add(dbcMBean.checkLeadScoringDBConnection());
        checkRes.add(hdfsAcMBean.checkHDFSAccess());
        checkRes.add(hdfsRcMBean.checkHDFSResource());
        checkRes.add(httpFSMBean.checkHttpAccess());

        for (String res : checkRes) {
            if (checkFailure(res))
                sb.append(res + "\n");
        }
        if (sb.toString().length() > prefix.length())
            return sb.toString();
        return "All dataplatform checks passed!";
    }

    public boolean checkFailure(String result) {
        if (result.contains("Failed") || result.contains("disabled"))
            return true;
        return false;
    }
}
