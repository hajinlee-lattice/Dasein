package com.latticeengines.datacloud.match.actors.visitor.impl;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.inject.Inject;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.collections4.MapUtils;
import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
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
import com.latticeengines.datacloud.match.util.EntityMatchUtils;
import com.latticeengines.domain.exposed.datacloud.match.EntityMatchKeyRecord;
import com.latticeengines.domain.exposed.datacloud.match.MatchKey;
import com.latticeengines.domain.exposed.datacloud.match.MatchKeyTuple;
import com.latticeengines.domain.exposed.datacloud.match.MatchKeyUtils;
import com.latticeengines.domain.exposed.datacloud.match.OperationalMode;
import com.latticeengines.domain.exposed.query.BusinessEntity;

@Component("accountMatchPlannerMicroEngineActor")
@Scope("prototype")
public class AccountMatchPlannerMicroEngineActor extends ExecutorMicroEngineTemplate {

    @Value("${datacloud.match.planner.account.executors.num}")
    private int executorNum;

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

    @Override
    protected void validateTraveler(Traveler traveler) {
        MatchTraveler matchTraveler = (MatchTraveler) traveler;
        // First make sure this actor should be run on this traveler.
        if (!OperationalMode.isEntityMatch(matchTraveler.getMatchInput().getOperationalMode())) {
            throw new RuntimeException(this.getClass().getSimpleName() + " called when not in Entity Match.");
        }
        if (!BusinessEntity.Account.name().equals(matchTraveler.getEntity())) {
            throw new UnsupportedOperationException(this.getClass().getSimpleName()
                    + " only handles Account entity, but found " + matchTraveler.getEntity());
        }
        if (CollectionUtils.isEmpty(matchTraveler.getInputDataRecord())) {
            throw new IllegalArgumentException("MatchTraveler must have input record data to generate MatchKeyTuple.");
        }
        if (MapUtils.isEmpty(matchTraveler.getEntityKeyPositionMaps())) {
            throw new IllegalArgumentException("MatchTraveler must have EntityKeyPositionMap set.");
        }

        if (MapUtils.isEmpty(matchTraveler.getMatchInput().getEntityKeyMaps())) {
            throw new IllegalArgumentException("MatchTraveler's MatchInput EntityKeyMaps should not be empty");
        }
    }

    @Override
    protected boolean accept(Traveler traveler) {
        MatchTraveler matchTraveler = (MatchTraveler) traveler;
        // If MatchKeyTuple is set up already, standardization is already done
        if (matchTraveler.getMatchKeyTuple() != null) {
            return false;
        }
        return true;
    }

    @Override
    protected int getExecutorNum() {
        return executorNum;
    }

    @Override
    protected void execute(Traveler traveler) {
        MatchTraveler matchTraveler = (MatchTraveler) traveler;
        List<Object> inputRecord = matchTraveler.getInputDataRecord();
        Map<MatchKey, List<Integer>> keyPositionMap = matchTraveler.getEntityKeyPositionMaps()
                .getOrDefault(BusinessEntity.Account.name(), new HashMap<>());
        EntityMatchKeyRecord entityMatchKeyRecord = matchTraveler.getEntityMatchKeyRecord();

        matchStandardizationService.parseRecordForNameLocation(inputRecord, keyPositionMap, null, entityMatchKeyRecord);
        matchStandardizationService.parseRecordForDuns(inputRecord, keyPositionMap, entityMatchKeyRecord);
        matchStandardizationService.parseRecordForDomain(inputRecord, keyPositionMap,
                matchTraveler.getMatchInput().isPublicDomainAsNormalDomain(), entityMatchKeyRecord);

        MatchKeyTuple matchKeyTuple = MatchKeyUtils.createAccountMatchKeyTuple(entityMatchKeyRecord);

        Map<MatchKey, List<String>> keyMap = EntityMatchUtils.getKeyMapForEntity(matchTraveler.getMatchInput(),
                BusinessEntity.Account.name());
        matchStandardizationService.parseRecordForSystemIds(inputRecord, keyMap, keyPositionMap, matchKeyTuple,
                entityMatchKeyRecord);

        // Send domain data to DomainCollectionService since this was not done outside
        // the Actor system.
        String domain = matchKeyTuple.getDomain();
        if (StringUtils.isNotBlank(domain)) {
            domainCollectService.enqueue(domain);
        }

        matchKeyTuple
                .setDomainFromMultiCandidates(matchStandardizationService.hasMultiDomain(inputRecord, keyPositionMap));
        matchTraveler.setMatchKeyTuple(matchKeyTuple);
        matchTraveler.addEntityMatchKeyTuple(BusinessEntity.Account.name(), matchKeyTuple);
        matchTraveler.addEntityMatchKeyTuple(BusinessEntity.LatticeAccount.name(), matchKeyTuple);
    }
}
