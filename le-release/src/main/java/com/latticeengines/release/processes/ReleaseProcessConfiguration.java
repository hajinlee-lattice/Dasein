package com.latticeengines.release.processes;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import com.latticeengines.release.exposed.activities.Activity;

@Configuration
public class ReleaseProcessConfiguration {

    @Autowired
    private Activity startReleaseNotificationActivity;

    @Autowired
    private Activity uploadProjectsToNexusActivity;

    @Autowired
    private Activity runReleaseProcessActivity;

    @Autowired
    private Activity dpDeploymentJobActivity;

    @Autowired
    private Activity jmxCheckActivity;

    @Autowired
    private Activity dpDeploymentTestActivity;

    @Autowired
    private Activity scoringDeploymentTestActivity;

    @Autowired
    private Activity plsDeploymentJobActivity;

    @Autowired
    private Activity plsDeploymentTestActivity;

    @Autowired
    private Activity createChangeManagementJiraActivity;

    @Autowired
    private Activity finishReleaseNotificationActivity;

    private List<Activity> preReleaseActivities;

    private List<Activity> postReleaseActivities;

    @Bean(name = "releaseDPProcess")
    public ReleaseProcess createReleaseDPProcess() {
        init();
        List<Activity> releaseDPActivities = new ArrayList<>();
        releaseDPActivities.addAll(preReleaseActivities);
        releaseDPActivities.addAll(Arrays.asList(new Activity[] { dpDeploymentJobActivity, dpDeploymentTestActivity,
                jmxCheckActivity }));
        releaseDPActivities.addAll(postReleaseActivities);
        return new ReleaseProcess(releaseDPActivities);
    }

    @Bean(name = "releasePLSProcess")
    public ReleaseProcess createReleasePLSProcess() {
        init();
        List<Activity> releasePLSActivities = new ArrayList<>();
        releasePLSActivities.addAll(preReleaseActivities);
        releasePLSActivities.addAll(Arrays
                .asList(new Activity[] { plsDeploymentJobActivity, plsDeploymentTestActivity }));
        releasePLSActivities.addAll(postReleaseActivities);
        return new ReleaseProcess(releasePLSActivities);
    }

    @Bean(name = "releaseAllProductsProcess")
    public ReleaseProcess createReleaseAllProductProcess() {
        init();
        List<Activity> releaseAllActivities = new ArrayList<>();
        releaseAllActivities.addAll(preReleaseActivities);
        releaseAllActivities.addAll(Arrays.asList(new Activity[] { dpDeploymentJobActivity, plsDeploymentJobActivity,
                dpDeploymentTestActivity, jmxCheckActivity, plsDeploymentTestActivity }));
        releaseAllActivities.addAll(postReleaseActivities);
        return new ReleaseProcess(releaseAllActivities);
    }

    public void init() {
        preReleaseActivities = Arrays.asList(new Activity[] { startReleaseNotificationActivity, uploadProjectsToNexusActivity, runReleaseProcessActivity });
        postReleaseActivities = Arrays.asList(new Activity[] { finishReleaseNotificationActivity }); // createChangeManagementJiraActivity
    }
}
