package com.latticeengines.admin.dynamicopts;

import java.util.Collections;
import java.util.List;

public interface OptionsProvider {

    List<String> getOptions();

    class NullProvider implements OptionsProvider {
        @Override
        public List<String> getOptions() {
            return Collections.emptyList();
        }
    }

}
