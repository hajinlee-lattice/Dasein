package com.latticeengines.domain.exposed.eai;

import com.latticeengines.domain.exposed.BaseProperty;

public final class ExportProperty extends BaseProperty {

    protected ExportProperty() {
        throw new UnsupportedOperationException();
    }

    public static final String TARGET_FILE_NAME = "targetFileName";

    public static final String TARGET_DIRECTORY = "targetDirectory";

    public static final String INPUT_FILE_PATH = "inputFilePath";

    public static final String EXPORT_USING_DISPLAYNAME = "exportUsingDisplayName";

    public static final String EXPORT_EXCLUSION_COLUMNS = "exportExclusionColumns";
    public static final String EXPORT_INCLUSION_COLUMNS = "exportInclusionColumns";

    public static final String NUM_MAPPERS = "numMappers";
}
