package com.latticeengines.actors.visitor.sample;

import com.latticeengines.actors.exposed.traveler.GuideBook;
import com.latticeengines.actors.exposed.traveler.Traveler;

public class SampleTraveler extends Traveler {
    private boolean isSampleed;

    public SampleTraveler(String rootOperationUid, GuideBook guideBook) {
        super(rootOperationUid, guideBook);
    }

    public boolean isSampleed() {
        return isSampleed;
    }

    public void setSampleed(boolean isSampleed) {
        this.isSampleed = isSampleed;
    }

}
