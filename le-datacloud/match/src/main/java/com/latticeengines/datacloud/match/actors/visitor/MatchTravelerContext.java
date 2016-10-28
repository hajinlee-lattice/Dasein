package com.latticeengines.datacloud.match.actors.visitor;

import com.latticeengines.actors.exposed.traveler.GuideBook;
import com.latticeengines.actors.exposed.traveler.TravelerContext;

public class MatchTravelerContext extends TravelerContext {
    private boolean isMatched;

    public MatchTravelerContext(String rootOperationUid, GuideBook guideBook) {
        super(rootOperationUid, guideBook);
    }

    public boolean isMatched() {
        return isMatched;
    }

    public void setMatched(boolean isMatched) {
        this.isMatched = isMatched;
    }

}
