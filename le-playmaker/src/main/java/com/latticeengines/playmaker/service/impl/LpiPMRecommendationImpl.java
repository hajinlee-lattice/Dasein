package com.latticeengines.playmaker.service.impl;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;

import javax.annotation.PostConstruct;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import com.latticeengines.domain.exposed.metadata.InterfaceName;
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

    private Random rand = new Random(System.currentTimeMillis());

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
        Map<String, Pair<Long, String>> playNameAndPidMap = new HashMap<>();
        for (Play play : plays) {
            playNameAndPidMap.put(play.getName(), new ImmutablePair<>(play.getPid(), play.getName()));
        }

        Map<String, Long> playLaunchNameAndPidMap = new HashMap<>();

        if (CollectionUtils.isNotEmpty(data)) {
            int rowNum = offset + 1;

            for (Map<String, Object> accExtRec : data) {
                String playName = (String) accExtRec.get(PlaymakerConstants.PlayID);

                if (accExtRec.containsKey(PlaymakerConstants.PlayID)) {
                    accExtRec.put(PlaymakerConstants.PlayID, playNameAndPidMap.get(playName).getLeft());
                    accExtRec.put(PlaymakerConstants.PlayID + PlaymakerConstants.V2,
                            playNameAndPidMap.get(playName).getRight());
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
                    accExtRec.put(PlaymakerConstants.LaunchID + PlaymakerConstants.V2, launchName);
                }

                if (accExtRec.containsKey(PlaymakerConstants.LaunchDate)) {
                    accExtRec.put(PlaymakerConstants.ExpirationDate,
                            (long) accExtRec.get(PlaymakerConstants.LaunchDate) + 8000000L);
                }

                accExtRec.put(PlaymakerConstants.PriorityID, 25);
                accExtRec.put(PlaymakerConstants.SfdcContactID, "");
                accExtRec.put(PlaymakerConstants.Contacts,
                        createContacts((String) accExtRec.get(InterfaceName.CompanyName.name())));

                accExtRec.put(PlaymakerConstants.RowNum, rowNum++);

            }

        }

        return data;
    }

    private List<Map<String, String>> createContacts(String companyName) {
        List<Map<String, String>> contacts = new ArrayList<>();
        int randNum = rand.nextInt(10000);
        String firstName = "FirstName" + randNum;
        String lastName = "LastName" + randNum;

        Map<String, String> contact = new HashMap<>();
        String domain = createDummyDomain(companyName);

        contact.put(PlaymakerConstants.Email, firstName + "@" + domain);
        contact.put(PlaymakerConstants.Address, companyName + " Dr");
        contact.put(PlaymakerConstants.Phone, "248.813.2000");
        contact.put(PlaymakerConstants.State, "MI");
        contact.put(PlaymakerConstants.ZipCode, "48098-2815");
        contact.put(PlaymakerConstants.Country, "USA");
        contact.put(PlaymakerConstants.SfdcContactID, "");
        contact.put(PlaymakerConstants.City, "Troy");
        contact.put(PlaymakerConstants.ContactID, "" + randNum);
        contact.put(PlaymakerConstants.Name, firstName + " " + lastName);
        contacts.add(contact);
        return contacts;
    }

    private String createDummyDomain(String companyName) {
        String dot = ".";
        int maxDomainLength = 7;
        String com = "com";

        String domain = "";

        if (companyName != null) {
            domain = companyName.trim();
            domain = StringUtils.replace(domain, dot + dot, dot);
            if (domain.endsWith(dot)) {
                domain = domain.substring(0, domain.length() - 1);
            }
            domain = StringUtils.replace(domain, " ", dot);
            domain = domain.replaceAll("[^A-Za-z0-9]", dot);
            domain = StringUtils.replace(domain, " ", dot);
            if (domain.endsWith(dot + com)) {
                if (domain.length() > maxDomainLength) {
                    domain = domain.substring(domain.length() - maxDomainLength, domain.length());
                }
                if (domain.startsWith(dot)) {
                    domain = domain.substring(dot.length());
                }
                return domain;
            } else if (!domain.endsWith(dot)) {
                domain += dot;
            }
        }
        domain = domain + com;
        if (domain.length() > maxDomainLength) {
            domain = domain.substring(domain.length() - maxDomainLength, domain.length());
        }
        if (domain.startsWith(dot)) {
            domain = domain.substring(dot.length());
        }
        return domain;
    }

    @Override
    public int getRecommendationCount(long start, SynchronizationDestinationEnum syncDestination,
            List<String> playIds) {
        return recommendationEntityMgr.findRecommendationCount(LpiPMUtils.dateFromEpochSeconds(start),
                syncDestination.name(), playIds);
    }

}
