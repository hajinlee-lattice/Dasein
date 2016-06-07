package com.latticeengines.pls.mbean;

import java.util.concurrent.Callable;

import org.apache.hadoop.conf.Configuration;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import com.latticeengines.pls.entitymanager.SourceFileEntityMgr;
import com.latticeengines.pls.service.SourceFileService;
import com.latticeengines.pls.service.impl.SourceFilePurgeCallable;
import com.latticeengines.quartzclient.mbean.QuartzJobBean;
import com.latticeengines.security.exposed.entitymanager.TenantEntityMgr;

@Component("sourceFilePurge")
public class SourceFilePurgeBean implements QuartzJobBean {

    @Autowired
    private SourceFileService sourceFileService;

    @Autowired
    private SourceFileEntityMgr sourceFileEntityMgr;

    @Autowired
    private TenantEntityMgr tenantEntityMgr;

    @Autowired
    private Configuration yarnConfiguration;

    @Value("${pls.sourcefile.retain.days}")
    private int retainDays;

    @Override
    public Callable<Boolean> getCallable() {
        SourceFilePurgeCallable.Builder builder = new SourceFilePurgeCallable.Builder();
        builder.sourceFileService(sourceFileService) //
                .sourceFileEntityMgr(sourceFileEntityMgr) //
                .tenantEntityMgr(tenantEntityMgr) //
                .retainDays(retainDays);//
        return new SourceFilePurgeCallable(builder);
    }

}
