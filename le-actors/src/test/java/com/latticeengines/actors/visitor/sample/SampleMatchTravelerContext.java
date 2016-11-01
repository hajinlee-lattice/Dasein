package com.latticeengines.actors.visitor.sample;

import com.latticeengines.actors.exposed.traveler.GuideBook;
import com.latticeengines.actors.exposed.traveler.TravelContext;

public class SampleMatchTravelerContext extends TravelContext {
    private boolean isMatched;

    public SampleMatchTravelerContext(String rootOperationUid, GuideBook guideBook) {
        super(rootOperationUid);
    }

    public boolean isMatched() {
        return isMatched;
    }

    public void setMatched(boolean isMatched) {
        this.isMatched = isMatched;
    }

}
