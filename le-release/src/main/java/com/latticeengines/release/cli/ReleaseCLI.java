package com.latticeengines.release.cli;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.springframework.context.ApplicationContext;
import org.springframework.context.support.ClassPathXmlApplicationContext;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.latticeengines.release.exposed.domain.JenkinsParameters;
import com.latticeengines.release.exposed.domain.JiraParameters;
import com.latticeengines.release.exposed.domain.JenkinsParameters.NameValuePair;
import com.latticeengines.release.exposed.domain.JiraParameters.JiraFields;
import com.latticeengines.release.hipchat.service.HipChatService;
import com.latticeengines.release.hipchat.service.impl.HipChatServiceImpl;
import com.latticeengines.release.jenkins.service.JenkinsService;
import com.latticeengines.release.jenkins.service.impl.JenkinsServiceImpl;
import com.latticeengines.release.jira.service.ChangeManagementJiraService;
import com.latticeengines.release.jira.service.impl.ChangeManagementJiraServiceImpl;
import com.latticeengines.release.jmx.service.JMXCheckService;
import com.latticeengines.release.jmx.service.impl.JMXCheckServiceImpl;

public class ReleaseCLI {

    public static void main(String[] args) throws JsonProcessingException, IOException {
        ApplicationContext applicationContext = new ClassPathXmlApplicationContext("release-context.xml");

        //invokeJMX(applicationContext);
        //invokeHipChat(applicationContext);
        invokeJenkins(applicationContext);
        //invokeJira(applicationContext);
    }

    public static void invokeJMX(ApplicationContext applicationContext) {
        JMXCheckService jmxService = (JMXCheckServiceImpl) applicationContext.getBean("jmxService");
        System.out.println(jmxService.checkJMX());
    }

    public static void invokeHipChat(ApplicationContext applicationContext) {
        HipChatService hc = (HipChatServiceImpl) applicationContext.getBean("hipchatService");
        hc.sendNotification("red", "release failed");
    }

    public static void invokeJenkins(ApplicationContext applicationContext) throws JsonProcessingException, IOException {
        JenkinsService js = (JenkinsServiceImpl) applicationContext.getBean("jenkinsService");

        JenkinsParameters par = new JenkinsParameters();
        NameValuePair p1 = new NameValuePair();
        p1.setName("SVN_DIR");
        p1.setValue("tags");
        NameValuePair p2 = new NameValuePair();
        p2.setName("SVN_BRANCH_NAME");
        p2.setValue("release_2.0.4");
        List<NameValuePair> kvList = new ArrayList<>();
        kvList.add(p1);
        kvList.add(p2);
        par.setNameValuePairs(kvList);

        System.out.println(js.triggerJenkinsJobWithParameters("", par));
        JsonNode jn = js.getLastBuildStatus("");
        System.out.println(jn.get("building"));
        System.out.println(jn.get("result"));
        System.out.println(jn.get("number"));
    }

    public static void invokeJira(ApplicationContext applicationContext) {
        JiraParameters jp = new JiraParameters();

        Map<String, String> project = new HashMap<>();
        project.put("key", "CR");
        String summary = "testing rest api";
        Map<String, String> issueType = new HashMap<>();
        issueType.put("name", "Record");
        String backoutPlan = ".";
        JiraFields jf = new JiraFields(project, summary, issueType, backoutPlan);
        jp.setJiraFields(jf);

        ChangeManagementJiraService jira = (ChangeManagementJiraServiceImpl) applicationContext
                .getBean("changeManagmentJiraService");
        System.out.println(jira.createChangeManagementTicket(jp));
    }
}
