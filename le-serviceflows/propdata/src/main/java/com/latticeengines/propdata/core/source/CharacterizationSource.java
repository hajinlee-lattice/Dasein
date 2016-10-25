package com.latticeengines.propdata.core.source;

public interface CharacterizationSource extends FixedIntervalSource {

    String getVersionKey();

    String[] getAttrKey();

    String getTotalKey();

    String[] getGroupKeys();

    String[] getExcludeAttrs();
}
