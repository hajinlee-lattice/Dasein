package com.latticeengines.actors.visitor.sample;

import com.latticeengines.actors.exposed.traveler.GuideBook;
import com.latticeengines.actors.exposed.traveler.TravelerContext;

public class SampleMatchTravelerContext extends TravelerContext {
    private boolean isMatched;

    public SampleMatchTravelerContext(String rootOperationUid, GuideBook guideBook) {
        super(rootOperationUid, guideBook);
    }

    public boolean isMatched() {
        return isMatched;
    }

    public void setMatched(boolean isMatched) {
        this.isMatched = isMatched;
    }

}
