package com.latticeengines.datacloud.match.actors.visitor.impl;

import static com.latticeengines.datacloud.match.domain.TpsLookupResult.ReturnCode.Ok;

import java.util.List;

import javax.annotation.PostConstruct;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

import com.latticeengines.actors.exposed.traveler.Response;
import com.latticeengines.datacloud.match.actors.visitor.DataSourceMicroEngineTemplate;
import com.latticeengines.datacloud.match.actors.visitor.MatchTraveler;
import com.latticeengines.datacloud.match.domain.TpsLookupResult;
import com.latticeengines.domain.exposed.datacloud.match.MatchKeyTuple;

@Component("dunsToTpsMicroEngineActor")
@Scope("prototype")
public class DunsToTpsMicroEngineActor extends DataSourceMicroEngineTemplate<TpsLookupActor> {
    private static final Logger log = LoggerFactory.getLogger(DunsToTpsMicroEngineActor.class);

    @PostConstruct
    public void postConstruct() {
        log.info("Started actor: " + self());
    }

    @Override
    protected Class<TpsLookupActor> getDataSourceActorClz() {
        return TpsLookupActor.class;
    }

    @Override
    protected boolean accept(MatchTraveler traveler) {
        MatchKeyTuple matchKeyTuple = traveler.getMatchKeyTuple();
        return StringUtils.isNotBlank(matchKeyTuple.getDuns());
    }

    @Override
    protected void recordActorAndTuple(MatchTraveler traveler) {
    }

    @Override
    protected void process(Response response) {
        MatchTraveler traveler = (MatchTraveler) response.getTravelerContext();
        if (response.getResult() == null) {
            traveler.debug(String.format("Encountered an issue with TriPeopleSegment lookup at %s: %s.", //
                    this.getClass().getSimpleName(), "Result in response is empty"));
            return;
        }
        MatchKeyTuple matchKeyTuple = traveler.getMatchKeyTuple();
        String duns = matchKeyTuple.getDuns();
        TpsLookupResult res = (TpsLookupResult) response.getResult();
        TpsLookupResult.ReturnCode returnCode = res.getReturnCode();
        if (Ok.equals(returnCode)) {
            List<String> recordIds = res.getRecordIds();
            String logMessage = String.format("Found %d tps records for duns %s", recordIds.size(), duns);
            traveler.debug(logMessage);
            traveler.setResult(recordIds);
            traveler.setMatched(true);
        } else {
            traveler.warn(String.format("Unsuccessful tps lookup for duns %s: %s", duns, returnCode));
        }
    }
}
