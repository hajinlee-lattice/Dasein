package com.latticeengines.playmaker.service.impl;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.annotation.PostConstruct;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang3.tuple.ImmutableTriple;
import org.apache.commons.lang3.tuple.Triple;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import com.latticeengines.domain.exposed.metadata.InterfaceName;
import com.latticeengines.domain.exposed.playmaker.PlaymakerConstants;
import com.latticeengines.domain.exposed.playmaker.PlaymakerUtils;
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
        return postProcess(recommendationEntityMgr.findRecommendationsAsMap(PlaymakerUtils.dateFromEpochSeconds(start),
                offset, maximum, syncDestination.name(), playIds), offset);
    }

    private List<Map<String, Object>> postProcess(List<Map<String, Object>> data, int offset) {

        List<Play> plays = internalResourceRestApiProxy.getPlays(MultiTenantContext.getCustomerSpace());
        Map<String, Triple<Long, String, String>> playNameAndPidMap = new HashMap<>();
        for (Play play : plays) {
            playNameAndPidMap.put(play.getName(),
                    new ImmutableTriple<>(play.getPid(), play.getDisplayName(), play.getDescription()));
        }

        Map<String, Long> playLaunchNameAndPidMap = new HashMap<>();

        if (CollectionUtils.isNotEmpty(data)) {
            int rowNum = offset + 1;

            for (Map<String, Object> accExtRec : data) {

                if (accExtRec.containsKey(PlaymakerConstants.AccountID)) {
                    String accountId = (String) accExtRec.get(PlaymakerConstants.AccountID);
                    if (StringUtils.isNotBlank(accountId) && StringUtils.isNumeric(accountId)) {
                        Long longAccId = Long.parseLong(accountId);
                        accExtRec.put(PlaymakerConstants.AccountID, longAccId);
                    }
                }

                String playName = (String) accExtRec.get(PlaymakerConstants.PlayID);

                if (accExtRec.containsKey(PlaymakerConstants.PlayID)) {
                    accExtRec.put(PlaymakerConstants.PlayID, playNameAndPidMap.get(playName).getLeft());
                    accExtRec.put(PlaymakerConstants.PlayID + PlaymakerConstants.V2, playName);
                    accExtRec.put(PlaymakerConstants.DisplayName, playNameAndPidMap.get(playName).getMiddle());

                    if (accExtRec.get(PlaymakerConstants.Description) == null) {
                        accExtRec.put(PlaymakerConstants.Description, playNameAndPidMap.get(playName).getRight());
                    }
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

                List<Map<String, String>> contactList = PlaymakerUtils
                        .getExpandedContacts((String) accExtRec.get(PlaymakerConstants.Contacts));

                accExtRec.put(PlaymakerConstants.Contacts, //
                        contactList.isEmpty() //
                                ? PlaymakerUtils
                                        .createDummyContacts((String) accExtRec.get(InterfaceName.CompanyName.name())) //
                                : contactList);

                accExtRec.put(PlaymakerConstants.RowNum, rowNum++);

            }

        }

        return data;
    }

    @Override
    public int getRecommendationCount(long start, SynchronizationDestinationEnum syncDestination,
            List<String> playIds) {
        return recommendationEntityMgr.findRecommendationCount(PlaymakerUtils.dateFromEpochSeconds(start),
                syncDestination.name(), playIds);
    }

}
