package com.latticeengines.pls.controller.datacollection;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;

import javax.inject.Inject;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.security.access.prepost.PreAuthorize;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.servlet.ModelAndView;
import org.springframework.web.servlet.view.json.MappingJackson2JsonView;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableMap;
import com.latticeengines.common.exposed.util.ThreadPoolUtils;
import com.latticeengines.db.exposed.util.MultiTenantContext;
import com.latticeengines.domain.exposed.cdl.DropBoxAccessMode;
import com.latticeengines.domain.exposed.cdl.DropBoxSummary;
import com.latticeengines.domain.exposed.cdl.GrantDropBoxAccessRequest;
import com.latticeengines.domain.exposed.cdl.GrantDropBoxAccessResponse;
import com.latticeengines.domain.exposed.pls.frontend.Status;
import com.latticeengines.domain.exposed.pls.frontend.UIAction;
import com.latticeengines.domain.exposed.pls.frontend.View;
import com.latticeengines.domain.exposed.security.Tenant;
import com.latticeengines.domain.exposed.security.User;
import com.latticeengines.monitor.exposed.service.EmailService;
import com.latticeengines.proxy.exposed.cdl.DropBoxProxy;
import com.latticeengines.security.exposed.AccessLevel;
import com.latticeengines.security.exposed.service.UserService;

import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;

@Api(value = "dropbox", description = "REST resource for atlas drop box")
@RestController
@RequestMapping("/dropbox")
@PreAuthorize("hasRole('View_PLS_CDL_Data')")
public class DropBoxResource {

    @Inject
    private DropBoxProxy dropBoxProxy;

    @Inject
    private UserService userService;

    @Inject
    private EmailService emailService;

    private static final Logger log = LoggerFactory.getLogger(DropBoxResource.class);

    public static final String GET_DROPBOX_SUCCESS_TITLE = "Automation Credential";
    private static final String GET_DROPBOX_SUCCESS_MSG = "<p>A new key has been generated. Please save it in a secure location. We do not store your credentials, "
            + "so you'll have to generate a new key if you lose it after you close this window.</p><br><p>Protocol:</p><p><strong>Amazon S3</strong></p><p>Access Key:</p><p><strong>%s</strong></p><p>Secret:</p><p><strong>%s</strong></p><br>"
            + "<p>Bucket:</p><p><strong>%s</strong></p><p>DropFolder:</p><p><strong>%s</strong></p><p>Tenant:</p><p><strong>%s</strong></p><p>Generated by:</p><p><strong>%s</strong></p><br><p>To learn more about the automating the file imports, visit http://help.lattice-engines.com</p>";
    public static final String GET_DROPBOX_WARNING_TITLE = "Warning";
    private static final String GET_DROPBOX_WARNING_MSG = "<p>Credentials have already been granted.</p><p>Your Access Key is <strong>%s</strong></p><br>"
            + "<p>Are you sure you want to generate new credential? The existing key will expire immediately, and all admins will be notified.</p>";

    private ExecutorService tpForParallelStream = ThreadPoolUtils.getCachedThreadPool("dropbox-resource");;

    @GetMapping("")
    @ResponseBody
    @ApiOperation(value = "Get drop box summary")
    @PreAuthorize("hasRole('Generate_S3_Credential')")
    public ModelAndView getDropBox() {
        MappingJackson2JsonView jsonView = new MappingJackson2JsonView();
        String customerSpace = MultiTenantContext.getShortTenantId();
        UIAction uiAction = generateUIActionBasedOnDropBox(dropBoxProxy.getDropBox(customerSpace));
        return new ModelAndView(jsonView, ImmutableMap.of(UIAction.class.getSimpleName(), uiAction));
    }

    @VisibleForTesting
    UIAction generateUIActionBasedOnDropBox(DropBoxSummary dropBoxSummary) {
        String customerSpace = MultiTenantContext.getShortTenantId();
        UIAction uiAction = new UIAction();
        if (dropBoxSummary == null) {
            throw new RuntimeException("Tenant " + customerSpace //
                    + " does not have a dropbox.");
        }

        if (dropBoxSummary.getAccessKeyId() == null) {
            GrantDropBoxAccessRequest request = new GrantDropBoxAccessRequest();
            request.setAccessMode(DropBoxAccessMode.LatticeUser);
            log.info(String.format("Grant access via request %s for tenant %s.", request.toString(), customerSpace));
            GrantDropBoxAccessResponse response = dropBoxProxy.grantAccess(customerSpace, request);
            sendEmailToAdmins(response);
            uiAction.setTitle(GET_DROPBOX_SUCCESS_TITLE);
            uiAction.setView(View.Modal);
            uiAction.setStatus(Status.Info);
            uiAction.setMessage(String.format(GET_DROPBOX_SUCCESS_MSG, response.getAccessKey(), response.getSecretKey(),
                    response.getBucket(), response.getDropBox(), MultiTenantContext.getShortTenantId(),
                    MultiTenantContext.getEmailAddress()));
        } else {
            uiAction.setTitle(GET_DROPBOX_WARNING_TITLE);
            uiAction.setView(View.Modal);
            uiAction.setStatus(Status.Warning);
            uiAction.setMessage(String.format(GET_DROPBOX_WARNING_MSG, dropBoxSummary.getAccessKeyId()));
        }
        return uiAction;
    }

    private void sendEmailToAdmins(GrantDropBoxAccessResponse response) {
        Tenant tenant = MultiTenantContext.getTenant();
        String initiator = MultiTenantContext.getEmailAddress();
        List<User> users = userService.getUsers(tenant.getId());
        List<Runnable> runnables = new ArrayList<>();
        users.stream().forEach(user -> {
            if (AccessLevel.EXTERNAL_ADMIN.name().equals(user.getAccessLevel())) {
                Runnable runnable = () -> {
                    emailService.sendS3CredentialEmail(user, tenant, response, initiator);
                };
                runnables.add(runnable);
            }
        });
        log.info(String.format("Sending emails to %d external admins starts initiated by %s", runnables.size(),
                initiator));

        if (tpForParallelStream == null) {
            tpForParallelStream = ThreadPoolUtils.getFixedSizeThreadPool("dropbox-resource", 4);
        }
        ThreadPoolUtils.runRunnablesInParallel(tpForParallelStream, runnables, 10, 1);
        log.info(String.format("Sending emails to %d external admins initiated by %s finishes", runnables.size(),
                initiator));
    }

    @PutMapping("/key")
    @ResponseBody
    @ApiOperation(value = "Refresh access key to drop box, if the access was granted to a Lattice user")
    @PreAuthorize("hasRole('Generate_S3_Credential')")
    public ModelAndView refreshAccessKey(@RequestBody GrantDropBoxAccessRequest request) {
        String customerSpace = MultiTenantContext.getShortTenantId();
        MappingJackson2JsonView jsonView = new MappingJackson2JsonView();
        UIAction uiAction = new UIAction();
        GrantDropBoxAccessResponse response = dropBoxProxy.refreshAccessKey(customerSpace);
        sendEmailToAdmins(response);
        uiAction.setTitle(GET_DROPBOX_SUCCESS_TITLE);
        uiAction.setView(View.Modal);
        uiAction.setStatus(Status.Info);
        uiAction.setMessage(String.format(GET_DROPBOX_SUCCESS_MSG, response.getAccessKey(), response.getSecretKey(),
                response.getBucket(), response.getDropBox(), MultiTenantContext.getShortTenantId(),
                MultiTenantContext.getEmailAddress()));
        return new ModelAndView(jsonView, ImmutableMap.of(UIAction.class.getSimpleName(), uiAction));
    }

}
