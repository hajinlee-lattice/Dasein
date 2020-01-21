package com.latticeengines.datacloud.match.actors.visitor.impl;

import static com.latticeengines.domain.exposed.query.BusinessEntity.Contact;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.inject.Inject;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

import com.latticeengines.actors.exposed.ActorSystemTemplate;
import com.latticeengines.actors.exposed.traveler.GuideBook;
import com.latticeengines.actors.exposed.traveler.Traveler;
import com.latticeengines.datacloud.match.actors.framework.MatchActorSystem;
import com.latticeengines.datacloud.match.actors.framework.MatchGuideBook;
import com.latticeengines.datacloud.match.actors.visitor.MatchTraveler;
import com.latticeengines.datacloud.match.service.MatchStandardizationService;
import com.latticeengines.datacloud.match.util.EntityMatchUtils;
import com.latticeengines.domain.exposed.datacloud.match.EntityMatchKeyRecord;
import com.latticeengines.domain.exposed.datacloud.match.MatchKey;
import com.latticeengines.domain.exposed.datacloud.match.MatchKeyTuple;
import com.latticeengines.domain.exposed.datacloud.match.MatchKeyUtils;
import com.latticeengines.domain.exposed.datacloud.match.OperationalMode;

@Component("contactMatchPlannerMicroEngineActor")
@Scope("prototype")
public class ContactMatchPlannerMicroEngineActor extends PlannerMicroEngineActorBase {

    @Value("${datacloud.match.planner.contact.executors.num}")
    private int executorNum;

    @Autowired
    @Qualifier("matchActorSystem")
    protected MatchActorSystem matchActorSystem;

    @Autowired
    @Qualifier("matchGuideBook")
    protected MatchGuideBook guideBook;

    @Inject
    private MatchStandardizationService matchStandardizationService;

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
    protected void validateTraveler(Traveler traveler) {
        MatchTraveler matchTraveler = (MatchTraveler) traveler;
        if (!OperationalMode.isEntityMatch(matchTraveler.getMatchInput().getOperationalMode())) {
            throw new RuntimeException(this.getClass().getSimpleName() + " called when not in Entity Match.");
        }
        if (!Contact.name().equals(matchTraveler.getEntity())) {
            throw new UnsupportedOperationException(this.getClass().getSimpleName()
                    + " only handles Contact entity, but found " + matchTraveler.getEntity());
        }
    }

    @Override
    protected void standardizeMatchFields(MatchTraveler traveler) {
        List<Object> inputRecord = traveler.getInputDataRecord();
        Map<MatchKey, List<Integer>> keyPositionMap = traveler.getEntityKeyPositionMaps()
                .getOrDefault(Contact.name(), new HashMap<>());
        EntityMatchKeyRecord entityMatchKeyRecord = traveler.getEntityMatchKeyRecord();

        matchStandardizationService.parseRecordForContact(inputRecord, keyPositionMap, entityMatchKeyRecord);
        MatchKeyTuple matchKeyTuple = MatchKeyUtils.createContactMatchKeyTuple(entityMatchKeyRecord);
        Map<MatchKey, List<String>> keyMap = EntityMatchUtils.getKeyMapForEntity(traveler.getMatchInput(),
                Contact.name());
        // MatchKeyTuple.SystemIds is updated during parsing
        matchStandardizationService.parseRecordForSystemIds(inputRecord, keyMap, keyPositionMap, matchKeyTuple,
                entityMatchKeyRecord);
        matchStandardizationService.parseRecordForPreferredEntityId(Contact.name(), inputRecord, keyPositionMap,
                entityMatchKeyRecord);

        traveler.setMatchKeyTuple(matchKeyTuple);
        traveler.addEntityMatchKeyTuple(Contact.name(), matchKeyTuple);
    }
}
