/*
 * Copyright (c) 2007-2015 Concurrent, Inc. All Rights Reserved.
 *
 * Project and contact information: http://www.cascading.org/
 *
 * This file is part of the Cascading project.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package cascading.jdbc;

import java.lang.reflect.Type;
import java.sql.Time;
import java.util.ArrayList;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import cascading.tuple.Fields;
import cascading.util.Util;

/**
 * Adds in the Distribution Key and Sort Keys columns that are specific to
 * Redshift. See AWS's docs for info. Note that these columns must exist as
 * defined column; they can't be keys that aren't in the columnNames list.
 */
public class RedshiftTableDesc extends TableDesc {

    private static final long serialVersionUID = -3792499487098752666L;

    private static final Logger LOG = LoggerFactory.getLogger(RedshiftTap.class);

    private String distStyle;
    private String distributionkey;
    private String sortKeyType;
    private String[] sortKeys;

    public RedshiftTableDesc(String tableName, String[] columnNames, String[] columnDefs,
            String distributionkey, String[] sortKeys) {
        super(tableName, columnNames, columnDefs, null);
        this.distributionkey = distributionkey;
        this.sortKeys = sortKeys;
    }

    public RedshiftTableDesc(String tableName, String[] columnNames, String[] columnDefs,
            String distStyle, String distributionkey, String sortKeyType, String[] sortKeys) {
        this(tableName, columnNames, columnDefs, distributionkey, sortKeys);
        this.distStyle = distStyle;
        this.sortKeyType = sortKeyType;
    }

    @Override
    public String getCreateTableStatement() {
        List<String> createTableStatement = new ArrayList<String>();

        createTableStatement = addCreateTableBodyTo(createTableStatement);
        String createTableCommand = String.format(getCreateTableFormat(), getTableName(),
                Util.join(createTableStatement, ", "), getRedshiftTableKeys());
        LOG.info("Creating table: " + createTableCommand);
        return createTableCommand;
    }

    @Override
    public String[] getPrimaryKeys() {
        return null;
    }

    @Override
    protected List<String> addCreateTableBodyTo(List<String> createTableStatement) {
        createTableStatement = addDefinitionsTo(createTableStatement);

        return createTableStatement;
    }

    public Fields getHFSFields() {
        String[] columnDefs = getColumnDefs();
        if (columnDefs == null)
            return Fields.ALL;

        Type[] types = new Type[columnDefs.length];

        for (int i = 0; i < columnDefs.length; i++)
            try {
                types[i] = findHFSTypeFor(columnDefs[i]);
            } catch (ClassNotFoundException exception) {
                LOG.error("unable to find HFS type for: {}. defaulting to string", columnDefs[i]);
                types[i] = String.class;
            }

        return new Fields(getColumnNames(), types);
    }

    public static Type findHFSTypeFor(String fieldName) throws ClassNotFoundException {
        if ("int".equals(fieldName))
            return int.class;
        else if ("int not null".equalsIgnoreCase(fieldName))
            return Integer.class;
        else if (fieldName != null && fieldName.startsWith("varchar"))
            return String.class;
        else if ("time".equalsIgnoreCase(fieldName))
            return Time.class;
        else if ("date".equalsIgnoreCase(fieldName))
            return String.class;
        else if ("timestamp".equalsIgnoreCase(fieldName))
            return String.class;
        else
            return String.class;
    }

    protected String getCreateTableFormat() {
        return "CREATE TABLE %s ( %s ) %s";
    }

    private String getRedshiftTableKeys() {
        StringBuilder sb = new StringBuilder().append("");
        if (distStyle != null)
            sb.append(" DISTSTYLE ").append(distStyle);
        if (distributionkey != null)
            sb.append(" DISTKEY (").append(distributionkey).append(") ");
        if (sortKeyType != null) {
            sb.append(" ").append(sortKeyType).append(" ");
        }
        if (sortKeys != null && sortKeys.length > 0)
            sb.append(" SORTKEY (").append(Util.join(sortKeys, ",")).append(") ");

        return sb.toString();
    }

}
