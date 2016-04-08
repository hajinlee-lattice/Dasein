package com.latticeengines.propdata.core.source;

import java.io.Serializable;

/*
 * 
 */
public interface Source extends Serializable {

    /*
     * name of the source
     */
    String getSourceName();

    /*
     * timestamp field for sorting
     */
    String getTimestampField();

    /*
     * primary key
     */
    String[] getPrimaryKey();

    /*
     * cron expression used to specify frequency of source data engine for this
     * source
     */
    String getDefaultCronExpression();

}
