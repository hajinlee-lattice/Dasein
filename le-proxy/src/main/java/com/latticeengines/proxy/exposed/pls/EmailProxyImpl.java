package com.latticeengines.proxy.exposed.pls;

import java.util.ArrayList;
import java.util.List;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import com.latticeengines.common.exposed.util.PropertyUtils;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.cdl.AtlasExport;
import com.latticeengines.domain.exposed.cdl.OrphanRecordsExportRequest;
import com.latticeengines.domain.exposed.cdl.S3ImportEmailInfo;
import com.latticeengines.domain.exposed.dcp.UploadEmailInfo;
import com.latticeengines.domain.exposed.pls.AdditionalEmailInfo;
import com.latticeengines.domain.exposed.pls.MetadataSegmentExport;
import com.latticeengines.domain.exposed.pls.PlayLaunch;
import com.latticeengines.domain.exposed.pls.PlayLaunchChannel;
import com.latticeengines.proxy.exposed.BaseRestApiProxy;

/*
 * this suppress warning is for DeprecatedBaseRestApiProxy class
 * the plan is to remove all the proxy that uses DeprecatedBaseRestApiProxy in the future,
 * so suppress for now.
 */
@Component("emailProxy")
public class EmailProxyImpl extends BaseRestApiProxy implements EmailProxy {

    private static final Logger log = LoggerFactory.getLogger(EmailProxyImpl.class);

    private static final String INSIGHTS_PATH = "/insights";

    private static final String PLS_INTERNAL_ENRICHMENT = "/internal/enrichment";

    public EmailProxyImpl() {
        super(PropertyUtils.getProperty("common.pls.url"), "pls");
    }


    public EmailProxyImpl(String hostPort) {
        super(hostPort, "pls");
    }

    @Override
    public boolean sendS3ImportEmail(String result, String tenantId, S3ImportEmailInfo emailInfo) {
        try {
            String url = constructUrl(combine("/internal/emails/s3import/result", result, tenantId));
            log.info(String.format("Putting to %s", url));
            return put("sendS3ImportEmail", url, emailInfo, Boolean.class);
        } catch (Exception e) {
            throw new RuntimeException("sendS3ImportEmail: Remote call failure", e);
        }
    }

    @Override
    public boolean sendS3TemplateUpdateEmail(String tenantId, S3ImportEmailInfo emailInfo) {
        try {
            String url = constructUrl(combine("/internal/emails/s3template/update", tenantId));
            log.info(String.format("Putting to %s", url));
            return put("sendS3TemplateUpdateEmail", url, emailInfo, Boolean.class);
        } catch (Exception e) {
            throw new RuntimeException("sendS3TemplateUpdateEmail: Remote call failure", e);
        }
    }

    @Override
    public boolean sendS3TemplateCreateEmail(String tenantId, S3ImportEmailInfo emailInfo) {
        try {
            String url = constructUrl(combine("/internal/emails/s3template/create", tenantId));
            log.info(String.format("Putting to %s", url));
            return put("sendS3TemplateCreateEmail", url, emailInfo, Boolean.class);
        } catch (Exception e) {
            throw new RuntimeException("sendS3TemplateCreateEmail: Remote call failure", e);
        }
    }

    public void deleteTenant(CustomerSpace customerSpace) {
        try {
            String url = constructUrl(combine("/admin/tenants/", customerSpace.toString()));
            log.debug(String.format("Deleting to %s", url));
            delete("deleteTenant", url);
        } catch (Exception e) {
            throw new RuntimeException("deleteTenant: Remote call failure", e);
        }
    }

    @Override
    public boolean sendCDLProcessAnalyzeEmail(String result, String tenantId, AdditionalEmailInfo info) {
        try {
            String url = constructUrl(combine("/internal/emails/processanalyze/result", result, tenantId));
            log.info(String.format("Putting to %s", url));
            return put("sendCDLProcessAnalyzeEmail", url, info, Boolean.class);
        } catch (Exception e) {
            throw new RuntimeException("sendProcessAnalyzeEmail: Remote call failure", e);
        }
    }

    @Override
    public void sendMetadataSegmentExportEmail(String result, String tenantId, MetadataSegmentExport export) {
        try {
            String url = constructUrl(combine("/internal/emails/segmentexport/result", result, tenantId));
            log.info(String.format("Putting to %s", url));
            put("sendMetadataSegmentExportEmail", url, export);
        } catch (Exception e) {
            throw new RuntimeException("sendMetadataSegmentExportEmail: Remote call failure", e);
        }
    }

    @Override
    public void sendAtlasExportEmail(String result, String tenantId, AtlasExport export) {
        try {
            String url = constructUrl(combine("/internal/emails/atlasexport/result", result, tenantId));
            log.info(String.format("Putting to %s", url));
            put("sendMetadataSegmentExportEmail", url, export);
        } catch (Exception e) {
            throw new RuntimeException("sendAtlasExportEmail: Remote call failure", e);
        }
    }

    @Override
    public void sendOrphanRecordsExportEmail(String result, String tenantId, OrphanRecordsExportRequest export) {
        try {
            String url = constructUrl(combine("/internal/emails/orphanexport/result", result, tenantId));
            log.info(String.format("Putting to %s", url));
            put("sendOrphanRecordsExportEmail", url, export);
        } catch (Exception e) {
            throw new RuntimeException("sendOrphanRecordsExportEmail: Remote call failure", e);
        }
    }

    @Override
    public void sendPlsCreateModelEmail(String result, String tenantId, AdditionalEmailInfo info) {
        try {
            String url = constructUrl(combine("/internal/emails/createmodel/result", result, tenantId));
            log.info(String.format("Putting to %s", url));
            put("sendPlsCreateModelEmail", url, info);
        } catch (Exception e) {
            throw new RuntimeException("sendCreateModelEmail: Remote call failure", e);
        }
    }

    @Override
    public void sendPlsScoreEmail(String result, String tenantId, AdditionalEmailInfo info) {
        try {
            String url = constructUrl(combine("/internal/emails/score/result", result, tenantId));
            log.info(String.format("Putting to %s", url));
            put("sendPlsScoreEmail", url, info);
        } catch (Exception e) {
            throw new RuntimeException("sendScoreEmail: Remote call failure", e);
        }
    }

    @Override
    public boolean sendPlayLaunchChannelExpiringEmail(String tenantId,
            PlayLaunchChannel playLaunchChannel) {
        try {
            String url = constructUrl(combine("/playlaunch/channel/expiring", tenantId));
            log.info(String.format("Putting to %s", url));
            return put("sendPlayLaunchChannelExpiringEmail", url, playLaunchChannel, Boolean.class);
        } catch (Exception e) {
            throw new RuntimeException("sendScoreEmail: Remote call failure", e);
        }
    }

    @Override
    public void sendPlayLaunchErrorEmail(String result, String tenantId, String user, PlayLaunch playLaunch) {
        try {
            String url = constructUrl(combine("/playlaunch/result/", result, tenantId));
            log.info(String.format("Putting to %s", url));
            List<String> params = new ArrayList<>();
            params.add("user=" + user);
            if (!params.isEmpty()) {
                url += "?" + StringUtils.join(params, "&");
            }
            put("sendPlayLaunchErrorEmail", url, playLaunch);
        } catch (Exception e) {
            throw new RuntimeException("sendScoreEmail: Remote call failure", e);
        }
    }

    @Override
    public void sendPlsEnrichInternalAttributeEmail(String result, String tenantId,
                                                    AdditionalEmailInfo info) {
        try {
            String url = constructUrl(combine("/internal/emails/enrichment/internal/result", result,
                    tenantId));
            log.info(String.format("Putting to %s", url));
            put("sendPlsEnrichInternalAttributeEmail", url, info);
        } catch (Exception e) {
            throw new RuntimeException("sendScoreEmail: Remote call failure", e);
        }
    }

    @Override
    public void sendUploadEmail(UploadEmailInfo uploadEmailInfo) {
        String url = constructUrl("/internal/emails/upload");
        log.info("Send upload email for Upload " + uploadEmailInfo.getUploadId());
        put("Send Upload Email", url, uploadEmailInfo);
    }

}
