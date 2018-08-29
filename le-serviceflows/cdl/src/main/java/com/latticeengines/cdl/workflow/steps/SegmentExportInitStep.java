package com.latticeengines.cdl.workflow.steps;

import javax.annotation.PostConstruct;

import org.apache.hadoop.conf.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

import com.google.common.annotations.VisibleForTesting;
import com.latticeengines.cdl.workflow.steps.export.SegmentExportProcessorFactory;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.db.exposed.entitymgr.TenantEntityMgr;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.exception.LedpCode;
import com.latticeengines.domain.exposed.exception.LedpException;
import com.latticeengines.domain.exposed.pls.MetadataSegmentExport;
import com.latticeengines.domain.exposed.pls.MetadataSegmentExport.Status;
import com.latticeengines.domain.exposed.query.frontend.FrontEndRestriction;
import com.latticeengines.domain.exposed.security.Tenant;
import com.latticeengines.domain.exposed.serviceflows.leadprioritization.steps.SegmentExportStepConfiguration;
import com.latticeengines.proxy.exposed.pls.InternalResourceRestApiProxy;
import com.latticeengines.workflow.exposed.build.BaseWorkflowStep;

@Component("segmentExportInitStep")
@Scope(ConfigurableBeanFactory.SCOPE_PROTOTYPE)
public class SegmentExportInitStep extends BaseWorkflowStep<SegmentExportStepConfiguration> {

    private static final Logger log = LoggerFactory.getLogger(SegmentExportInitStep.class);

    @Autowired
    private TenantEntityMgr tenantEntityMgr;

    @Autowired
    private SegmentExportProcessorFactory segmentExportProcessorFactory;

    @Value("${yarn.pls.url}")
    private String internalResourceHostPort;

    private InternalResourceRestApiProxy internalResourceRestApiProxy;

    @PostConstruct
    public void init() {
        internalResourceRestApiProxy = new InternalResourceRestApiProxy(internalResourceHostPort);
    }

    @Override
    public void execute() {
        execute(yarnConfiguration);
    }

    public void execute(Configuration yarnConfiguration) {

        SegmentExportStepConfiguration config = getConfiguration();
        CustomerSpace customerSpace = config.getCustomerSpace();
        String exportId = config.getMetadataSegmentExportId();

        try {
            log.info("Inside SegmentExportInitStep execute()");
            Tenant tenant = tenantEntityMgr.findByTenantId(customerSpace.toString());

            log.info(String.format("For tenant: %s", customerSpace.toString()));
            log.info(String.format("For exportId: %s", exportId));
            MetadataSegmentExport metadataSegmentExport = configuration.getMetadataSegmentExport();
            if (metadataSegmentExport == null) {
                metadataSegmentExport = internalResourceRestApiProxy.getMetadataSegmentExport(customerSpace, exportId);
                config.setMetadataSegmentExport(metadataSegmentExport);
            }

            log.info(String.format("Processing MetadataSegmentExport: %s", JsonUtils.serialize(metadataSegmentExport)));

            FrontEndRestriction accountRestriction = metadataSegmentExport.getAccountFrontEndRestriction();
            FrontEndRestriction contactRestriction = metadataSegmentExport.getContactFrontEndRestriction();

            log.info(String.format("Processing accountRestriction: %s", JsonUtils.serialize(accountRestriction)));
            log.info(String.format("Processing contactRestriction: %s", JsonUtils.serialize(contactRestriction)));

            segmentExportProcessorFactory.getProcessor(metadataSegmentExport.getType()) //
                    .executeExportActivity(tenant, config, yarnConfiguration);

            internalResourceRestApiProxy.updateMetadataSegmentExport(customerSpace, exportId, Status.COMPLETED);
        } catch (Exception ex) {
            internalResourceRestApiProxy.updateMetadataSegmentExport(customerSpace, exportId, Status.FAILED);
            throw new LedpException(LedpCode.LEDP_18167, ex);
        }
    }

    @VisibleForTesting
    void setTenantEntityMgr(TenantEntityMgr tenantEntityMgr) {
        this.tenantEntityMgr = tenantEntityMgr;
    }

    @VisibleForTesting
    void setInternalResourceRestApiProxy(InternalResourceRestApiProxy internalResourceRestApiProxy) {
        this.internalResourceRestApiProxy = internalResourceRestApiProxy;
    }

    @VisibleForTesting
    void setSegmentExportProcessorFactory(SegmentExportProcessorFactory segmentExportProcessorFactory) {
        this.segmentExportProcessorFactory = segmentExportProcessorFactory;
    }
}
