package com.latticeengines.propdata.job;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.quartz.DisallowConcurrentExecution;
import org.quartz.JobExecutionContext;
import org.quartz.JobExecutionException;
import org.springframework.scheduling.quartz.QuartzJobBean;

import com.latticeengines.proxy.exposed.propdata.IngestionProxy;
import com.latticeengines.proxy.exposed.propdata.PublicationProxy;
import com.latticeengines.proxy.exposed.propdata.TransformationProxy;

@DisallowConcurrentExecution
public class EngineHeartBeat extends QuartzJobBean {

    private static final Log log = LogFactory.getLog(RefreshHeartBeat.class);

    private TransformationProxy transformationProxy;
    private PublicationProxy publicationProxy;
    private IngestionProxy ingestionProxy;

    @Override
    public void executeInternal(JobExecutionContext jobExecutionContext)
            throws JobExecutionException {

        try {
            log.info(this.getClass().getSimpleName() + " invoking publication proxy scan.");
            publicationProxy.scan("");
        } catch (Exception e) {
            log.error("Failed to scan publication engine", e);
        }

        try {
            log.info(this.getClass().getSimpleName() + " invoking transformation proxy scan.");
            transformationProxy.scan("");
        } catch (Exception e) {
            log.error("Failed to scan transformation engine", e);
        }

        try {
            log.info(this.getClass().getSimpleName() + " invoking ingestion proxy scan.");
            ingestionProxy.scan("");
        } catch (Exception e) {
            log.error("Failed to scan ingestion engine", e);
        }


    }

    // ==============================
    // for quartz detail bean
    // ==============================

    public void setTransformationProxy(TransformationProxy transformationProxy) {
        this.transformationProxy = transformationProxy;
    }

    public void setPublicationProxy(PublicationProxy publicationProxy) {
        this.publicationProxy = publicationProxy;
    }

    public void setIngestionProxy(IngestionProxy ingestionProxy) {
        this.ingestionProxy = ingestionProxy;
    }

}
