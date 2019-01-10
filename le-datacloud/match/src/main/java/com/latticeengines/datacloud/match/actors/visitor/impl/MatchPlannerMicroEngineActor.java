package com.latticeengines.datacloud.match.actors.visitor.impl;

import java.util.List;
import java.util.Map;
import javax.inject.Inject;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.collections4.MapUtils;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

import com.latticeengines.actors.exposed.ActorSystemTemplate;
import com.latticeengines.actors.exposed.traveler.GuideBook;
import com.latticeengines.actors.exposed.traveler.Traveler;
import com.latticeengines.actors.template.ExecutorMicroEngineTemplate;
import com.latticeengines.datacloud.match.actors.framework.MatchActorSystem;
import com.latticeengines.datacloud.match.actors.framework.MatchGuideBook;
import com.latticeengines.datacloud.match.actors.visitor.MatchTraveler;
import com.latticeengines.datacloud.match.exposed.service.DomainCollectService;
import com.latticeengines.datacloud.match.service.MatchStandardizationService;
import com.latticeengines.domain.exposed.datacloud.match.EntityMatchKeyRecord;
import com.latticeengines.domain.exposed.datacloud.match.MatchInput;
import com.latticeengines.domain.exposed.datacloud.match.MatchKey;
import com.latticeengines.domain.exposed.datacloud.match.MatchKeyTuple;
import com.latticeengines.domain.exposed.datacloud.match.MatchKeyUtils;
import com.latticeengines.domain.exposed.datacloud.match.OperationalMode;
import com.latticeengines.domain.exposed.query.BusinessEntity;

@Component("matchPlannerMicroEngineActor")
@Scope("prototype")
public class MatchPlannerMicroEngineActor extends ExecutorMicroEngineTemplate {
    private static final Logger log = LoggerFactory.getLogger(MatchPlannerMicroEngineActor.class);

    @Inject
    @Qualifier("matchActorSystem")
    protected MatchActorSystem matchActorSystem;

    @Inject
    @Qualifier("matchGuideBook")
    protected MatchGuideBook guideBook;

    @Inject
    private MatchStandardizationService matchStandardizationService;

    @Inject
    private DomainCollectService domainCollectService;

    @Override
    protected GuideBook getGuideBook() {
        return guideBook;
    }

    @Override
    protected ActorSystemTemplate getActorSystem() {
        return matchActorSystem;
    }

    @Override
    protected boolean isValidMessageType(Object msg) {
        return msg instanceof MatchTraveler;
    }

    // TODO(@Jonathan): implement details
    @Override
    protected boolean accept(Traveler traveler) {
        MatchTraveler matchTraveler = (MatchTraveler) traveler;

        // First make sure this actor should be run on this traveler.
        if (!OperationalMode.ENTITY_MATCH.equals(matchTraveler.getMatchInput().getOperationalMode())) {
            log.warn("MatchPlannerMicroEngineActor called when not in Entity Match.");
            return false;
        }

        // TODO(jwinter): Decide if we want to keep all these error condition checks.

        if (StringUtils.isBlank(matchTraveler.getEntity())) {
            throw new IllegalArgumentException("MatchTraveler needs an entity set for Match Planning.");
        }

        if (!BusinessEntity.Account.name().equals(matchTraveler.getEntity())) {
            throw new UnsupportedOperationException(
                    "MatchPlannerMicroEngineActor can currently only handle Account entities.");
        }

        // For Entity Match, the MatchKeyTuple should not yet be set.
        if (matchTraveler.getMatchKeyTuple() != null) {
            throw new IllegalArgumentException("MatchTraveler should not have MatchKeyTuple set for Entity Match.");
        }

        if (CollectionUtils.isEmpty(matchTraveler.getInputDataRecord())) {
            throw new IllegalArgumentException("MatchTraveler must have input record data to generate MatchKeyTuple.");
        }

        if (MapUtils.isEmpty(matchTraveler.getEntityKeyPositionMaps())) {
            throw new IllegalArgumentException("MatchTraveler must have EntityKeyPositionMap set.");
        }

        if (MapUtils.isEmpty(matchTraveler.getEntityKeyPositionMaps().get(matchTraveler.getEntity()))) {
            throw new IllegalArgumentException("MatchTraveler EntityKeyPositionMap must have map for set entity");
        }

        // Not sure if this is necessary, but for now double check KeyMap.
        if (MapUtils.isEmpty(matchTraveler.getMatchInput().getEntityKeyMaps())) {
            throw new IllegalArgumentException("MatchTraveler's MatchInput EntityKeyMaps should not be empty");
        }
        Map<String, MatchInput.EntityKeyMap> entityKeyMaps = matchTraveler.getMatchInput().getEntityKeyMaps();
        if (!entityKeyMaps.containsKey(matchTraveler.getEntity())) {
            throw new IllegalArgumentException("MatchTraveler missing EntityMatchKey map for match entity "
                    + matchTraveler.getEntity());
        }
        if (MapUtils.isEmpty(entityKeyMaps.get(matchTraveler.getEntity()).getKeyMap())) {
            throw new IllegalArgumentException("MatchTraveler missing MatchKey map for match entity "
                    + matchTraveler.getEntity());
        }

        return true;
    }

    // TODO(@Jonathan): change to property file to make it configurable & actual
    // number to be decided based on performance tuning
    @Override
    protected int getExecutorNum() {
        return 4;
    }

    @Override
    protected void execute(Traveler traveler) {
        MatchTraveler matchTraveler = (MatchTraveler) traveler;
        List<Object> inputRecord = matchTraveler.getInputDataRecord();
        Map<MatchKey, List<Integer>> keyPositionMap = matchTraveler.getEntityKeyPositionMaps().get(matchTraveler
                .getEntity());
        EntityMatchKeyRecord entityMatchKeyRecord = matchTraveler.getEntityMatchKeyRecord();

        matchStandardizationService.parseRecordForNameLocation(inputRecord, keyPositionMap, null,
                entityMatchKeyRecord);
        matchStandardizationService.parseRecordForDuns(inputRecord, keyPositionMap, entityMatchKeyRecord);
        matchStandardizationService.parseRecordForDomain(inputRecord, keyPositionMap, null,
                matchTraveler.getMatchInput().isPublicDomainAsNormalDomain(), entityMatchKeyRecord);

        MatchKeyTuple matchKeyTuple = MatchKeyUtils.createMatchKeyTuple(entityMatchKeyRecord);

        Map<MatchKey, List<String>> keyMap = matchTraveler.getMatchInput().getEntityKeyMaps()
                .get(matchTraveler.getEntity()).getKeyMap();
        matchStandardizationService.parseRecordForSystemIds(inputRecord, keyMap, keyPositionMap, matchKeyTuple);

        // Send domain data to DomainCollectionService since this was not done outside the Actor system.
        String domain = matchKeyTuple.getDomain();
        if (StringUtils.isNotBlank(domain)) {
            domainCollectService.enqueue(domain);
        }

        matchTraveler.setMatchKeyTuple(matchKeyTuple);
    }
}
