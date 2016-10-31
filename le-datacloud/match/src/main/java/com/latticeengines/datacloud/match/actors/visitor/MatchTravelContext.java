package com.latticeengines.datacloud.match.actors.visitor;

import com.latticeengines.actors.exposed.traveler.GuideBook;
import com.latticeengines.actors.exposed.traveler.TravelContext;

public class MatchTravelContext extends TravelContext {
    private boolean isMatched;

    public MatchTravelContext(String rootOperationUid, GuideBook guideBook) {
        super(rootOperationUid, guideBook);
    }

    public boolean isMatched() {
        return isMatched;
    }

    public void setMatched(boolean isMatched) {
        this.isMatched = isMatched;
    }

}
