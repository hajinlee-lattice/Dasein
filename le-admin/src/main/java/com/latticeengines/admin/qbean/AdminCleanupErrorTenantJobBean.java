package com.latticeengines.admin.qbean;

import java.util.concurrent.Callable;

import javax.inject.Inject;

import org.apache.hadoop.conf.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import com.latticeengines.admin.service.impl.AdminCleanupErrorTenantJobCallable;
import com.latticeengines.aws.s3.S3Service;
import com.latticeengines.quartzclient.qbean.QuartzJobBean;

@Component("adminCleanupErrorTenantJob")
public class AdminCleanupErrorTenantJobBean implements QuartzJobBean {
    private static final Logger log = LoggerFactory.getLogger(AdminCleanupErrorTenantJobBean.class);

    @Inject
    private com.latticeengines.admin.service.TenantService adminTenantService;

    @Inject
    private com.latticeengines.security.exposed.service.TenantService tenantService;

    @Inject
    private Configuration yarnConfiguration;

    @Inject
    private S3Service s3Service;

    @Override
    public Callable<Boolean> getCallable(String jobArguments) {
        log.info(String.format("Got callback with job arguments = %s", jobArguments));
        AdminCleanupErrorTenantJobCallable.Builder builder = new AdminCleanupErrorTenantJobCallable.Builder();
        builder.jobArguments(jobArguments).adminTenantService(adminTenantService)
                .securityTenantService(tenantService).yarnConfiguration(yarnConfiguration).s3Service(s3Service);

        return new AdminCleanupErrorTenantJobCallable(builder);
    }
}
