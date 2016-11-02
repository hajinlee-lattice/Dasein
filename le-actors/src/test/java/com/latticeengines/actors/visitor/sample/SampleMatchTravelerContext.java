package com.latticeengines.actors.visitor.sample;

import com.latticeengines.actors.exposed.traveler.GuideBook;
import com.latticeengines.actors.exposed.traveler.Traveler;

public class SampleMatchTravelerContext extends Traveler {
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

    @Override
    protected Object getInputData() {
        // TODO Auto-generated method stub
        return null;
    }

}
