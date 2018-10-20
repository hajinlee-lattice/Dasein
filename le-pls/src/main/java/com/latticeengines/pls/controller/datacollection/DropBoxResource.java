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

    private static final String GET_DROPBOX_SUCCESS_MSG = "The credentials have been generated and emailed to all admin users.";
    private static final String GET_DROPBOX_WARNING_MSG = "Credentials have already been granted.\nYour Access Key is %s\n\n\\"
            + "Are you sure you want to generate new credential?\nThe existing key will expire immediately, and all admins will be notified.";

    private ExecutorService tpForParallelStream = ThreadPoolUtils.getCachedThreadPool("dropbox-resource");;

    @GetMapping("")
    @ResponseBody
    @ApiOperation(value = "Get drop box summary")
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
            request.setAccessMode(dropBoxSummary.getAccessMode());
            request.setExistingUser(dropBoxSummary.getLatticeUser());
            log.info(String.format("Grant access via request %s for tenant %s.", request.toString(), customerSpace));
            GrantDropBoxAccessResponse response = dropBoxProxy.grantAccess(customerSpace, request);
            sendEmailToAdmins(dropBoxSummary, response);
            uiAction.setView(View.Banner);
            uiAction.setStatus(Status.Success);
            uiAction.setMessage(GET_DROPBOX_SUCCESS_MSG);
        } else {
            uiAction.setView(View.Modal);
            uiAction.setStatus(Status.Warning);
            uiAction.setMessage(String.format(GET_DROPBOX_WARNING_MSG, dropBoxSummary.getAccessKeyId()));
        }
        return uiAction;
    }

    private void sendEmailToAdmins(DropBoxSummary dropBoxSummary, GrantDropBoxAccessResponse response) {
        Tenant tenant = MultiTenantContext.getTenant();
        List<User> users = userService.getUsers(tenant.getId());
        List<Runnable> runnables = new ArrayList<>();
        users.stream().forEach(user -> {
            if (AccessLevel.EXTERNAL_ADMIN.name().equals(user.getAccessLevel())) {
                Runnable runnable = () -> {
                    emailService.sendS3CredentialEmail(user, tenant, dropBoxSummary, response);
                };
                runnables.add(runnable);
            }
        });
        log.info(String.format("Sending emails to %d external admins starts", runnables.size()));

        if (tpForParallelStream == null) {
            tpForParallelStream = ThreadPoolUtils.getFixedSizeThreadPool("dropbox-resource", 4);
        }
        ThreadPoolUtils.runRunnablesInParallel(tpForParallelStream, runnables, 10, 1);
        log.info(String.format("Sending emails to %d external admins finishes", runnables.size()));
    }

    @PutMapping("/key")
    @ResponseBody
    @ApiOperation(value = "Refresh access key to drop box, if the access was granted to a Lattice user")
    @PreAuthorize("hasRole('Edit_PLS_CDL_Data')")
    public ModelAndView refreshAccessKey(@RequestBody GrantDropBoxAccessRequest request) {
        String customerSpace = MultiTenantContext.getShortTenantId();
        MappingJackson2JsonView jsonView = new MappingJackson2JsonView();
        UIAction uiAction = new UIAction();
        GrantDropBoxAccessResponse response = dropBoxProxy.refreshAccessKey(customerSpace);
        DropBoxSummary dropBoxSummary = new DropBoxSummary();
        dropBoxSummary.setBucket(response.getBucket());
        dropBoxSummary.setDropBox(response.getDropBox());
        sendEmailToAdmins(dropBoxSummary, response);
        uiAction.setView(View.Banner);
        uiAction.setStatus(Status.Success);
        uiAction.setMessage(GET_DROPBOX_SUCCESS_MSG);
        return new ModelAndView(jsonView, ImmutableMap.of(UIAction.class.getSimpleName(), uiAction));
    }

}
