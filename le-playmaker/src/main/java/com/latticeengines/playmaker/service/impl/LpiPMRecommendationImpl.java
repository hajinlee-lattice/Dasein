package com.latticeengines.playmaker.service.impl;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.annotation.PostConstruct;

import org.apache.commons.collections.CollectionUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import com.latticeengines.domain.exposed.playmaker.PlaymakerConstants;
import com.latticeengines.domain.exposed.playmakercore.SynchronizationDestinationEnum;
import com.latticeengines.domain.exposed.pls.Play;
import com.latticeengines.domain.exposed.pls.PlayLaunch;
import com.latticeengines.playmaker.service.LpiPMRecommendation;
import com.latticeengines.playmakercore.entitymanager.RecommendationEntityMgr;
import com.latticeengines.proxy.exposed.pls.InternalResourceRestApiProxy;
import com.latticeengines.security.exposed.util.MultiTenantContext;

@Component("lpiPMRecommendation")
public class LpiPMRecommendationImpl implements LpiPMRecommendation {

    @Autowired
    private RecommendationEntityMgr recommendationEntityMgr;

    @Value("${common.pls.url}")
    private String internalResourceHostPort;

    private InternalResourceRestApiProxy internalResourceRestApiProxy;

    @PostConstruct
    public void init() {
        internalResourceRestApiProxy = new InternalResourceRestApiProxy(internalResourceHostPort);
    }

    @Override
    public List<Map<String, Object>> getRecommendations(long start, int offset, int maximum,
            SynchronizationDestinationEnum syncDestination, List<String> playIds) {
        return postProcess(recommendationEntityMgr.findRecommendationsAsMap(LpiPMUtils.dateFromEpochSeconds(start),
                offset, maximum, syncDestination.name(), playIds), offset);
    }

    private List<Map<String, Object>> postProcess(List<Map<String, Object>> data, int offset) {

        List<Play> plays = internalResourceRestApiProxy.getPlays(MultiTenantContext.getCustomerSpace());
        Map<String, Long> playNameAndPidMap = new HashMap<>();
        for (Play play : plays) {
            playNameAndPidMap.put(play.getName(), play.getPid());
        }

        Map<String, Long> playLaunchNameAndPidMap = new HashMap<>();

        if (CollectionUtils.isNotEmpty(data)) {
            int rowNum = offset + 1;

            for (Map<String, Object> accExtRec : data) {
                String playName = (String) accExtRec.get(PlaymakerConstants.PlayID);

                if (accExtRec.containsKey(PlaymakerConstants.PlayID)) {
                    accExtRec.put(PlaymakerConstants.PlayID, playNameAndPidMap.get(playName));
                }

                if (accExtRec.containsKey(PlaymakerConstants.LaunchID)) {
                    String launchName = (String) accExtRec.get(PlaymakerConstants.LaunchID);
                    if (!playLaunchNameAndPidMap.containsKey(launchName)) {
                        PlayLaunch launch = internalResourceRestApiProxy
                                .getPlayLaunch(MultiTenantContext.getCustomerSpace(), playName, launchName);
                        if (launch != null) {
                            playLaunchNameAndPidMap.put(launchName, launch.getPid());
                        }
                    }
                    accExtRec.put(PlaymakerConstants.LaunchID, playLaunchNameAndPidMap.get(launchName));
                }

                if (accExtRec.containsKey(PlaymakerConstants.LaunchDate)) {
                    accExtRec.put(PlaymakerConstants.ExpirationDate,
                            (long) accExtRec.get(PlaymakerConstants.LaunchDate) + 8000000L);
                }

                accExtRec.put(PlaymakerConstants.PriorityID, 25);
                accExtRec.put(PlaymakerConstants.SfdcContactID, "");
                accExtRec.put(PlaymakerConstants.Contacts,

                        createContacts());

                accExtRec.put(PlaymakerConstants.RowNum, rowNum++);

            }

        }

        return data;
    }

    private List<Map<String, String>> createContacts() {
        List<Map<String, String>> contacts = new ArrayList<>();
        Map<String, String> contact = new HashMap<>();
        contact.put(PlaymakerConstants.Email, "tom.james@C2education.com");
        contact.put(PlaymakerConstants.Address, "5725 Delphi Drive");
        contact.put(PlaymakerConstants.Phone, "248.813.2000");
        contact.put(PlaymakerConstants.State, "MI");
        contact.put(PlaymakerConstants.ZipCode, "48098-2815");
        contact.put(PlaymakerConstants.Country, "USA");
        contact.put(PlaymakerConstants.SfdcContactID, "");
        contact.put(PlaymakerConstants.City, "Troy");
        contact.put(PlaymakerConstants.ContactID, "17");
        contact.put(PlaymakerConstants.Name, "Tom James");
        contacts.add(contact);
        return contacts;
    }

    @Override
    public int getRecommendationCount(long start, SynchronizationDestinationEnum syncDestination,
            List<String> playIds) {
        return recommendationEntityMgr.findRecommendationCount(LpiPMUtils.dateFromEpochSeconds(start),
                syncDestination.name(), playIds);
    }

}
