package com.latticeengines.propdata.core.source;

public interface CharacterizationSource extends FixedIntervalSource {

    String getVersionKey();

    String getAttrKey();

    String getCategoryKey();

    String getCountKey();

    String getPercentKey();

    String[] getGroupKeys();

    String[] getExcludeAttrs();
}
