package com.latticeengines.pls.controller;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;
import java.util.Map;

import javax.inject.Inject;
import javax.servlet.http.HttpServletRequest;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;

import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.cdl.AtlasExport;
import com.latticeengines.domain.exposed.cdl.OrphanRecordsExportRequest;
import com.latticeengines.domain.exposed.cdl.S3ImportEmailInfo;
import com.latticeengines.domain.exposed.dcp.UploadEmailInfo;
import com.latticeengines.domain.exposed.pls.AdditionalEmailInfo;
import com.latticeengines.domain.exposed.pls.AtlasExportType;
import com.latticeengines.domain.exposed.pls.ChannelSummary;
import com.latticeengines.domain.exposed.pls.LaunchState;
import com.latticeengines.domain.exposed.pls.LaunchSummary;
import com.latticeengines.domain.exposed.pls.MetadataSegmentExport;
import com.latticeengines.domain.exposed.pls.ModelSummary;
import com.latticeengines.domain.exposed.security.Tenant;
import com.latticeengines.domain.exposed.security.TenantEmailNotificationLevel;
import com.latticeengines.domain.exposed.security.TenantEmailNotificationType;
import com.latticeengines.domain.exposed.security.User;
import com.latticeengines.monitor.exposed.service.EmailService;
import com.latticeengines.pls.service.dcp.UploadService;
import com.latticeengines.proxy.exposed.lp.ModelSummaryProxy;
import com.latticeengines.security.exposed.AccessLevel;
import com.latticeengines.security.exposed.service.TenantService;
import com.latticeengines.security.exposed.service.UserService;

import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;

@Api(value = "emails", description = "REST resource for email operations")
@RestController
@RequestMapping("/internal/emails")
public class EmailResource {

    private static final Logger log = LoggerFactory.getLogger(EmailResource.class);

    public static final String TENANT_ID_PATH = "{tenantId:\\w+\\.\\w+\\.\\w+}";

    @Inject
    private ModelSummaryProxy modelSummaryProxy;

    @Inject
    private UserService userService;

    @Inject
    private TenantService tenantService;

    @Inject
    private EmailService emailService;

    @Inject
    private UploadService uploadService;

    @Value("${security.app.public.url:http://localhost:8081}")
    private String appPublicUrl;

    @PutMapping("/createmodel/result/{result}/" + TENANT_ID_PATH)
    @ResponseBody
    @ApiOperation(value = "Send out email after model creation")
    public void sendPlsCreateModelEmail(@PathVariable("result") String result,
                                        @PathVariable("tenantId") String tenantId, @RequestBody AdditionalEmailInfo emailInfo) {
        List<User> users = userService.getUsers(tenantId);
        String modelName = emailInfo.getModelId();
        if (modelName != null && !modelName.isEmpty()) {
            for (User user : users) {
                if (user.getEmail().equals(emailInfo.getUserId())) {
                    String tenantName = tenantService.findByTenantId(tenantId).getName();
                    switch (result.toUpperCase()) {
                        case "COMPLETED":
                            if (user.getAccessLevel().equals(AccessLevel.INTERNAL_ADMIN.name())
                                    || user.getAccessLevel().equals(AccessLevel.INTERNAL_USER.name())) {
                                emailService.sendPlsCreateModelCompletionEmail(user, appPublicUrl, tenantName, modelName,
                                        true);
                            } else {
                                emailService.sendPlsCreateModelCompletionEmail(user, appPublicUrl, tenantName, modelName,
                                        false);
                            }
                            break;
                        case "FAILED":
                            if (user.getAccessLevel().equals(AccessLevel.INTERNAL_ADMIN.name())
                                    || user.getAccessLevel().equals(AccessLevel.INTERNAL_USER.name())) {
                                emailService.sendPlsCreateModelErrorEmail(user, appPublicUrl, tenantName, modelName, true);
                            } else {
                                emailService.sendPlsCreateModelErrorEmail(user, appPublicUrl, tenantName, modelName, false);
                            }
                            break;
                        default:
                            log.warn(String.format(
                                    "Non-completed nor failed model created. Model status: %s, "
                                            + "Tenant ID: %s, Details: %s",
                                    result, tenantId, JsonUtils.serialize(emailInfo)));
                            break;
                    }
                }
            }
        }
    }

    @PutMapping("/score/result/{result}/" + TENANT_ID_PATH)
    @ResponseBody
    @ApiOperation(value = "Send out email after scoring")
    public void sendPlsScoreEmail(@PathVariable("result") String result, @PathVariable("tenantId") String tenantId,
                                  @RequestBody AdditionalEmailInfo emailInfo, HttpServletRequest request) {
        List<User> users = userService.getUsers(tenantId);
        String modelId = emailInfo.getModelId();
        if (modelId != null && !modelId.isEmpty()) {
            for (User user : users) {
                if (user.getEmail().equals(emailInfo.getUserId())) {
                    String tenantName = tenantService.findByTenantId(tenantId).getName();
                    ModelSummary modelSummary = modelSummaryProxy.getByModelId(modelId);
                    if (modelSummary != null) {
                        String modelName = modelSummary.getDisplayName();
                        if ("COMPLETED".equals(result)) {
                            if (user.getAccessLevel().equals(AccessLevel.INTERNAL_ADMIN.name())
                                    || user.getAccessLevel().equals(AccessLevel.INTERNAL_USER.name())) {
                                emailService.sendPlsScoreCompletionEmail(user, appPublicUrl, tenantName, modelName,
                                        true);
                            } else {
                                emailService.sendPlsScoreCompletionEmail(user, appPublicUrl, tenantName, modelName,
                                        false);
                            }
                        } else if ("FAILED".equals(result)) {
                            if (user.getAccessLevel().equals(AccessLevel.INTERNAL_ADMIN.name())
                                    || user.getAccessLevel().equals(AccessLevel.INTERNAL_USER.name())) {
                                emailService.sendPlsScoreErrorEmail(user, appPublicUrl, tenantName, modelName, true);
                            } else {
                                emailService.sendPlsScoreErrorEmail(user, appPublicUrl, tenantName, modelName, false);
                            }
                        }
                    }
                }
            }
        }
    }

    @PutMapping("/enrichment/internal/result/{result}/" + TENANT_ID_PATH)
    @ResponseBody
    @ApiOperation(value = "Send out email after enrichment of internal attributes")
    public void sendPlsInternalEnrichmentEmail(@PathVariable("result") String result,
                                               @PathVariable("tenantId") String tenantId, @RequestBody AdditionalEmailInfo emailInfo,
                                               HttpServletRequest request) {
        List<User> users = userService.getUsers(tenantId);
        String modelId = emailInfo.getModelId();
        if (modelId != null && !modelId.isEmpty()) {
            for (User user : users) {
                if (user.getEmail().equals(emailInfo.getUserId())) {
                    String tenantName = tenantService.findByTenantId(tenantId).getName();
                    ModelSummary modelSummary = modelSummaryProxy.getByModelId(modelId);
                    if (modelSummary != null) {
                        String modelName = modelSummary.getDisplayName();
                        if ("COMPLETED".equals(result)) {
                            emailService.sendPlsEnrichInternalAttributeCompletionEmail(user, appPublicUrl, tenantName,
                                    modelName, emailInfo.getExtraInfoList());
                        } else if ("FAILED".equals(result)) {
                            emailService.sendPlsEnrichInternalAttributeErrorEmail(user, appPublicUrl, tenantName,
                                    modelName, emailInfo.getExtraInfoList());
                        }
                    }
                }
            }
        }
    }

    @PutMapping("/processanalyze/result/{result}/" + TENANT_ID_PATH)
    @ResponseBody
    @ApiOperation(value = "Send out email after processanalyze")
    public boolean sendCDLProcessAnalyzeEmail(@PathVariable("result") String result,
                                              @PathVariable("tenantId") String tenantId, @RequestBody AdditionalEmailInfo emailInfo) {
        boolean isSendEmail = false;
        List<User> users = userService.getUsers(tenantId);
        Tenant tenant = tenantService.findByTenantId(tenantId);
        boolean onlyCurrentUser = tenant.getNotificationType().equals(TenantEmailNotificationType.SINGLE_USER);
        for (User user : users) {
            if ("COMPLETED".equals(result)
                    && shouldSendEmail(tenant, TenantEmailNotificationLevel.INFO, "CDLProcessAnalyze")) {
                if ((!onlyCurrentUser && AccessLevel.EXTERNAL_ADMIN.name().equals(user.getAccessLevel()))
                        || user.getEmail().equals(emailInfo.getUserId())) {
                    emailService.sendCDLProcessAnalyzeCompletionEmail(user, tenant, appPublicUrl);
                    isSendEmail = true;
                }
            } else if ("FAILED".equals(result)
                    && shouldSendEmail(tenant, TenantEmailNotificationLevel.ERROR, "CDLProcessAnalyze")) {
                if ((!onlyCurrentUser && AccessLevel.EXTERNAL_ADMIN.name().equals(user.getAccessLevel()))
                        || user.getEmail().equals(emailInfo.getUserId())) {
                    emailService.sendCDLProcessAnalyzeErrorEmail(user, tenant, appPublicUrl);
                    isSendEmail = true;
                }
            }
        }
        return isSendEmail;
    }

    @PutMapping("/orphanexport/result/{result}/" + TENANT_ID_PATH)
    @ResponseBody
    @ApiOperation(value = "Send out email after orphan records export")
    public void sendOrphanRecordsExportEmail(@PathVariable String result, @PathVariable String tenantId,
                                             @RequestBody OrphanRecordsExportRequest exportRequest, HttpServletRequest request) {
        List<User> users = userService.getUsers(tenantId);
        String exportID = exportRequest.getExportId();
        String exportType = exportRequest.getOrphanRecordsType().getDisplayName();

        if (StringUtils.isNotBlank(exportID)) {
            for (User user : users) {
                if (user.getEmail().equals(exportRequest.getCreatedBy())) {
                    String tenantName = tenantService.findByTenantId(tenantId).getName();
                    String url = String.format("%s/atlas/tenant/%s/orphanexport/%s", appPublicUrl, tenantName,
                            exportID);
                    log.info(String.format("URL=%s, result=%s", url, result));
                    switch (result) {
                        case "READY":
                            emailService.sendPlsExportOrphanRecordsSuccessEmail(user, tenantName, appPublicUrl, url,
                                    exportID, exportType);
                            break;
                        case "GENERATING":
                            emailService.sendPlsExportOrphanRecordsRunningEmail(user, exportID, exportType);
                            break;
                        default:
                            log.warn("Unknown result {}", result);
                    }
                }
            }
        }
    }

    @PutMapping("/segmentexport/result/{result}/" + TENANT_ID_PATH)
    @ResponseBody
    @ApiOperation(value = "Send out email after segment export")
    public void sendSegmentExportEmail(@PathVariable("result") String result, @PathVariable("tenantId") String tenantId,
                                       @RequestBody MetadataSegmentExport export) {
        List<User> users = userService.getUsers(tenantId);
        String exportID = export.getExportId();
        AtlasExportType exportType = export.getType();
        sendEmail(exportType, exportID, users, result, tenantId, export.getCreatedBy());
    }

    private void sendEmail(AtlasExportType exportType, String exportID, List<User> users, String result,
                           String tenantId, String createBy) {
        String exportTypeStr = exportType.getDisplayName();
        if (exportID != null && !exportID.isEmpty()) {
            for (User user : users) {
                if (user.getEmail().equals(createBy)) {
                    String tenantName = tenantService.findByTenantId(tenantId).getName();
                    String url = appPublicUrl + "/atlas/tenant/" + tenantName + "/export/" + exportID;
                    switch (result) {
                        case "COMPLETED":
                            emailService.sendPlsExportSegmentSuccessEmail(user, url, exportID, exportTypeStr, tenantName);
                            break;
                        case "FAILED":
                            emailService.sendPlsExportSegmentErrorEmail(user, exportID, exportTypeStr);
                            break;
                        case "STARTED":
                            emailService.sendPlsExportSegmentRunningEmail(user, exportID);
                            break;
                        default:
                            log.warn("Unknown result {}", result);
                    }
                }
            }
        }
    }

    @PutMapping("/atlasexport/result/{result}/" + TENANT_ID_PATH)
    @ResponseBody
    @ApiOperation(value = "Send out email after segment export")
    public void sendAtlasExportEmail(@PathVariable("result") String result, @PathVariable("tenantId") String tenantId,
                                     @RequestBody AtlasExport export) {
        List<User> users = userService.getUsers(tenantId);
        String exportID = export.getUuid();
        AtlasExportType exportType = export.getExportType();
        sendEmail(exportType, exportID, users, result, tenantId, export.getCreatedBy());
    }

    @PutMapping("/s3import/result/{result}/" + TENANT_ID_PATH)
    @ResponseBody
    @ApiOperation(value = "Send out email after s3 import")
    public boolean sendS3ImportEmail(@PathVariable("result") String result, @PathVariable("tenantId") String tenantId,
                                     @RequestBody S3ImportEmailInfo emailInfo) {
        boolean isSendEmail = false;
        List<User> users = userService.getUsers(tenantId);
        Tenant tenant = tenantService.findByTenantId(tenantId);
        boolean onlyCurrentUser = tenant.getNotificationType().equals(TenantEmailNotificationType.SINGLE_USER);
        log.info("tenant {} , tenant level: {},  notification_type: {}.", tenant.getId(),
                tenant.getNotificationLevel().name(), tenant.getNotificationType());
        for (User user : users) {
            if ((onlyCurrentUser && user.getEmail().equalsIgnoreCase(emailInfo.getUser()))
                    || (!onlyCurrentUser && user.getAccessLevel().equals(AccessLevel.EXTERNAL_ADMIN.name()))) {
                switch (result.toUpperCase()) {
                    case "FAILED":
                        if (shouldSendEmail(tenant, TenantEmailNotificationLevel.ERROR, "S3Import")) {
                            emailService.sendIngestionStatusEmail(user, tenant, appPublicUrl, result, emailInfo);
                            isSendEmail = true;
                        }
                        break;
                    case "SUCCESS":
                        if (shouldSendEmail(tenant, TenantEmailNotificationLevel.INFO, "S3Import")) {
                            emailService.sendIngestionStatusEmail(user, tenant, appPublicUrl, result, emailInfo);
                            isSendEmail = true;
                        }
                        break;
                    case "IN_PROGRESS":
                        if ((shouldSendEmail(tenant, TenantEmailNotificationLevel.INFO, "S3Import")
                                && StringUtils.isEmpty(emailInfo.getErrorMsg()))
                                || (shouldSendEmail(tenant, TenantEmailNotificationLevel.WARNING, "S3Import")
                                && StringUtils.isNotEmpty(emailInfo.getErrorMsg()))) {
                            emailService.sendIngestionStatusEmail(user, tenant, appPublicUrl, result, emailInfo);
                            isSendEmail = true;
                        }
                        break;
                    default:
                        break;
                }
            }
        }
        return isSendEmail;
    }

    @PutMapping("/s3template/create/" + TENANT_ID_PATH)
    @ResponseBody
    @ApiOperation(value = "Send out email after s3 template created")
    public boolean sendS3TemplateCreateEmail(@PathVariable("tenantId") String tenantId, @RequestBody S3ImportEmailInfo emailInfo) {
        boolean isSendEmail = false;
        List<User> users = userService.getUsers(tenantId);
        Tenant tenant = tenantService.findByTenantId(tenantId);
        if (shouldSendEmail(tenant, TenantEmailNotificationLevel.INFO, "S3TemplateCreate")) {
            boolean onlyCurrentUser = tenant.getNotificationType().equals(TenantEmailNotificationType.SINGLE_USER);
            for (User user : users) {
                if ((onlyCurrentUser && user.getEmail().equalsIgnoreCase(emailInfo.getUser()))
                        || (!onlyCurrentUser && user.getAccessLevel().equals(AccessLevel.EXTERNAL_ADMIN.name()))) {
                    emailService.sendS3TemplateCreateEmail(user, tenant, appPublicUrl, emailInfo);
                    isSendEmail = true;
                }
            }
        }
        return isSendEmail;
    }

    @PutMapping("/s3template/update/" + TENANT_ID_PATH)
    @ResponseBody
    @ApiOperation(value = "Send out email after s3 template update")
    public boolean sendS3TemplateUpdateEmail(@PathVariable("tenantId") String tenantId,
                                             @RequestBody S3ImportEmailInfo emailInfo) {
        boolean isSendEmail = false;
        List<User> users = userService.getUsers(tenantId);
        Tenant tenant = tenantService.findByTenantId(tenantId);
        if (shouldSendEmail(tenant, TenantEmailNotificationLevel.INFO, "S3TemplateUpdate")) {
            boolean onlyCurrentUser = tenant.getNotificationType().equals(TenantEmailNotificationType.SINGLE_USER);
            for (User user : users) {
                if ((onlyCurrentUser && user.getEmail().equalsIgnoreCase(emailInfo.getUser()))
                        || (!onlyCurrentUser && user.getAccessLevel().equals(AccessLevel.EXTERNAL_ADMIN.name()))) {
                    emailService.sendS3TemplateUpdateEmail(user, tenant, appPublicUrl, emailInfo);
                    isSendEmail = true;
                }
            }
        }
        return isSendEmail;
    }

    @PutMapping("/playlaunchchannel/expiring/" + TENANT_ID_PATH)
    @ResponseBody
    @ApiOperation(value = "Send out email warning Always On Campaign will expire soon")
    public boolean sendPlayLaunchChannelExpiringEmail(@PathVariable("tenantId") String tenantId,
            @RequestBody ChannelSummary playLaunchChannel) {
        boolean isSendEmail = false;
        List<User> users = userService.getUsers(tenantId);
        if (playLaunchChannel != null) {
            SimpleDateFormat format = new SimpleDateFormat("MMMM dd, yyyy 'at' hh:mm a z");
            String playDisplayName = playLaunchChannel.getPlayDisplayName();
            String externalSystemName = playLaunchChannel.getExternalSystemName();
            String nextScheduledLaunch = format.format(playLaunchChannel.getNextScheduledLaunch());
            String launchInterval;
            if (playLaunchChannel.getExpirationPeriodString().endsWith("W")) {
                launchInterval = "Week";
            } else if (playLaunchChannel.getExpirationPeriodString().endsWith("M")) {
                launchInterval = "Month";
            } else {
                log.warn("Unknown period String");
                return false;
            }

            for (User user : users) {
                if (user.getEmail().equals(playLaunchChannel.getUpdatedBy())) {
                    String tenantName = tenantService.findByTenantId(tenantId).getName();
                    String launchSettingsUrl = String.format("%s/atlas/tenant/%s/playbook/overview/%s",
                            appPublicUrl, tenantName, playLaunchChannel.getPlayName());
                    emailService.sendPlsAlwaysOnCampaignExpirationEmail(user, launchInterval, launchSettingsUrl,
                            playDisplayName,
                            externalSystemName, nextScheduledLaunch);
                    isSendEmail = true;
                }
            }
        }
        return isSendEmail;
    }

    @PutMapping("/playlaunch/failed/" + TENANT_ID_PATH)
    @ResponseBody
    @ApiOperation(value = "Send out email after Campaign launch error")
    public void sendPlayLaunchErrorEmail(
            @PathVariable("tenantId") String tenantId, @RequestBody LaunchSummary playLaunch,
            @RequestParam(value = "user", required = true) String userEmail) {
        List<User> users = userService.getUsers(tenantId);
        if (playLaunch != null) {
            SimpleDateFormat format = new SimpleDateFormat("MMMM dd, yyyy 'at' hh:mm a z");
            String playDisplayName = playLaunch.getPlayDisplayName();
            String externalSystemName = playLaunch.getDestinationSysName().getDisplayName();
            String playLaunchState = playLaunch.getUiLaunchState();
            String playLaunchCreated = format.format(playLaunch.getLaunchTime());
            String currentTime = format.format(new Date());
            for (User user : users) {
                if (user.getEmail().equals(userEmail)) {
                    String tenantName = tenantService.findByTenantId(tenantId).getName();
                    String launchHistoryUrl = String.format("%s/atlas/tenant/%s/playbook/dashboard/%s/launchhistory",
                            appPublicUrl, tenantName, playLaunch.getPlayName());
                    if (playLaunch.getLaunchState().equals(LaunchState.Failed)
                            || playLaunch.getLaunchState().equals(LaunchState.SyncFailed)) {
                        emailService.sendPlsCampaignFailedEmail(user, launchHistoryUrl, playDisplayName,
                                externalSystemName,
                                playLaunchState, playLaunchCreated, currentTime);
                    } else if (playLaunch.getLaunchState().equals(LaunchState.Canceled)) {
                        emailService.sendPlsCampaignCanceledEmail(user, launchHistoryUrl, playDisplayName,
                                externalSystemName,
                                playLaunchState, playLaunchCreated, currentTime);
                    } else {
                        log.warn(String.format(
                                "Non-failed nor canceled playLaunch triggered email logic. playLaunch status: %s, "
                                        + "Tenant ID: %s, Details: %s",
                                playLaunchState, tenantId, JsonUtils.serialize(playLaunch)));
                    }
                }
            }
        }
    }

    private boolean shouldSendEmail(Tenant tenant, TenantEmailNotificationLevel notificationLevel, String jobType) {
        Map<String, TenantEmailNotificationLevel> jobLevelFlags = tenant.getJobNotificationLevels();
        TenantEmailNotificationLevel flag = jobLevelFlags.containsKey(jobType)
                ? tenant.getJobNotificationLevels().get(jobType)
                : tenant.getNotificationLevel();
        return flag.compareTo(notificationLevel) >= 0;
    }

    @PutMapping("/upload")
    @ResponseBody
    @ApiOperation(value = "Send out email after upload state change")
    public void sendUploadEmail(@RequestBody UploadEmailInfo uploadEmailInfo) {
        uploadService.sendUploadEmail(uploadEmailInfo);
    }
}
