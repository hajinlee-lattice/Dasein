package com.latticeengines.release.cli;

import org.springframework.context.ApplicationContext;
import org.springframework.context.support.ClassPathXmlApplicationContext;

import com.latticeengines.release.hipchat.service.HipChatService;
import com.latticeengines.release.hipchat.service.impl.HipChatServiceImpl;
import com.latticeengines.release.jenkins.service.JenkinsService;
import com.latticeengines.release.jenkins.service.impl.JenkinsServiceImpl;
import com.latticeengines.release.jmx.service.JMXCheckService;
import com.latticeengines.release.jmx.service.impl.JMXCheckServiceImpl;

public class ReleaseCLI {
    
    public static void main(String[] args){
        ApplicationContext applicationContext = new ClassPathXmlApplicationContext("release-context.xml");
        JMXCheckService jmxService = (JMXCheckServiceImpl) applicationContext.getBean("jmxService");
        System.out.println(jmxService.checkJMX());
        
        //HipChatService hc = (HipChatServiceImpl) applicationContext.getBean("hipchatService");
        //hc.sendNotification("red", "release failed");
        
        JenkinsService js = (JenkinsServiceImpl) applicationContext.getBean("jenkinsService");
        System.out.println(js.triggerJenkinsJob().getStatusCode());
    }
}
