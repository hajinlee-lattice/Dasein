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
import com.latticeengines.datacloud.match.service.impl.InternalOutputRecord;
import com.latticeengines.domain.exposed.datacloud.match.MatchInput;
import com.latticeengines.domain.exposed.datacloud.match.MatchKey;
import com.latticeengines.domain.exposed.datacloud.match.MatchKeyTuple;
import com.latticeengines.domain.exposed.datacloud.match.NameLocation;
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

        if (CollectionUtils.isEmpty(matchTraveler.getInputRecord())) {
            throw new IllegalArgumentException("MatchTraveler must have input record data to generate MatchKeyTuple.");
        }

        // TODO(jwinter): Refactor code to avoid using InternalOutputRecord in actor system.
        // For now, we need an InternalOutputRecord in the MatchTraveler.
        if (matchTraveler.getInternalOutputRecord() == null) {
            throw new IllegalArgumentException("MatchTraveler must have InternalOutputRecord set for Match Planning");
        }

        if (MapUtils.isEmpty(matchTraveler.getInternalOutputRecord().getKeyPositionMap())) {
            throw new IllegalArgumentException("MatchTraveler must have InternalOutputRecord with KeyPositionMap.");
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

    // TODO(jwinter): Refactor to avoid needing InternalOutputRecord.
    @Override
    protected void execute(Traveler traveler) {
        MatchTraveler matchTraveler = (MatchTraveler) traveler;
        InternalOutputRecord matchRecord = matchTraveler.getInternalOutputRecord();
        List<Object> inputRecord = matchTraveler.getInputRecord();
        Map<MatchKey, List<Integer>> keyPositionMap = matchRecord.getKeyPositionMap();

        matchStandardizationService.parseRecordForNameLocation(inputRecord, keyPositionMap, null,
                matchRecord);
        matchStandardizationService.parseRecordForDuns(inputRecord, keyPositionMap, matchRecord);
        matchStandardizationService.parseRecordForDomain(inputRecord, keyPositionMap, null,
                matchTraveler.getMatchInput().isPublicDomainAsNormalDomain(), matchRecord);
        matchStandardizationService.parseRecordForLatticeAccountId(inputRecord, keyPositionMap, matchRecord);
        matchStandardizationService.parseRecordForLookupId(inputRecord, keyPositionMap, matchRecord);

        MatchKeyTuple matchKeyTuple = createMatchKeyTuple(matchRecord);

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

    // TODO(jwinter): This is temporary until we stop using InternalOutputRecord to pass in data to the actor system.
    private static MatchKeyTuple createMatchKeyTuple(InternalOutputRecord matchRecord) {
        MatchKeyTuple matchKeyTuple = new MatchKeyTuple();
        NameLocation nameLocationInfo = matchRecord.getParsedNameLocation();
        if (nameLocationInfo != null) {
            matchKeyTuple.setCity(nameLocationInfo.getCity());
            matchKeyTuple.setCountry(nameLocationInfo.getCountry());
            matchKeyTuple.setCountryCode(nameLocationInfo.getCountryCode());
            matchKeyTuple.setName(nameLocationInfo.getName());
            matchKeyTuple.setState(nameLocationInfo.getState());
            matchKeyTuple.setZipcode(nameLocationInfo.getZipcode());
            matchKeyTuple.setPhoneNumber(nameLocationInfo.getPhoneNumber());
        }
        if (!matchRecord.isPublicDomain() || matchRecord.isMatchEvenIsPublicDomain()) {
            matchKeyTuple.setDomain(matchRecord.getParsedDomain());
        }
        matchKeyTuple.setDuns(matchRecord.getParsedDuns());
        return matchKeyTuple;
    }
}
