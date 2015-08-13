package com.latticeengines.release.jmx.activities;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import com.latticeengines.release.error.handler.ErrorHandler;
import com.latticeengines.release.exposed.activities.BaseActivity;
import com.latticeengines.release.exposed.domain.StatusContext;
import com.latticeengines.release.jmx.service.JMXCheckService;

@Component("jmxCheckActivity")
public class JMXCheckActivity extends BaseActivity {

    @Autowired
    private JMXCheckService jmxCheckService;

    @Value("${release.jmx.rmi}")
    private String jmxRMI;

    @Value("${release.jmx.object}")
    private String objectName;

    @Value("${release.jmx.operation}")
    private String operation;

    @Autowired
    public JMXCheckActivity(@Qualifier("defaultErrorHandler") ErrorHandler errorHandler) {
        super(errorHandler);
    }

    @Override
    public StatusContext runActivity() {
        String response = jmxCheckService.checkJMX(jmxRMI, objectName, operation);
        statusContext.setResponseMessage(response);
        return statusContext;
    }

}
