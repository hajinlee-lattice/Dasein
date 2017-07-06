package com.latticeengines.domain.exposed.dataplatform;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Properties;
import java.util.Set;

import org.apache.commons.lang3.StringUtils;
import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormatter;
import org.joda.time.format.ISODateTimeFormat;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.modeling.DbCreds;

public class SqoopImporter {

    private static List<String> defaultHadoopArgs = Arrays.asList(
            "-Dmapreduce.task.timeout=600000", //
            "-Dmapreduce.job.running.map.limit=32", //
            "-Dmapreduce.tasktracker.map.tasks.maximum=32"
    );

    private static List<String> defaultOptions = Arrays.asList(
            "--relaxed-isolation", // 
            "--as-avrodatafile", //
            "--compress"
    );

    private static DateTimeFormatter dateTimeFormatter = ISODateTimeFormat.dateTime();

    private String table;
    private String query;
    private String targetDir;
    private DbCreds dbCreds;
    private String queue;
    private String customer;
    private int numMappers;
    private String splitColumn;
    private List<String> columnsToInclude;
    private boolean sync;
    private Properties properties;
    private List<String> hadoopArgs;
    private List<String> otherOptions;
    private Mode mode;

    @JsonProperty("table")
    public String getTable() {
        return table;
    }

    @JsonProperty("table")
    public void setTable(String table) {
        this.table = table;
    }

    @JsonProperty("query")
    public String getQuery() {
        return query;
    }

    @JsonProperty("query")
    public void setQuery(String query) {
        this.query = query;
    }

    @JsonProperty("target_dir")
    public String getTargetDir() {
        return targetDir;
    }

    @JsonProperty("target_dir")
    public void setTargetDir(String targetDir) {
        this.targetDir = targetDir;
    }

    @JsonProperty("db_creds")
    public DbCreds getDbCreds() {
        return dbCreds;
    }

    @JsonProperty("db_creds")
    public void setDbCreds(DbCreds dbCreds) {
        this.dbCreds = dbCreds;
    }

    @JsonProperty("queue")
    public String getQueue() {
        return queue;
    }

    @JsonProperty("queue")
    public void setQueue(String queue) {
        this.queue = queue;
        String queueArg = "-Dmapreduce.job.queuename=" + this.queue;
        List<String> args = new ArrayList<>();
        args.add(queueArg);
        if (this.hadoopArgs != null) {
            for (String arg : this.hadoopArgs) {
                if (!arg.contains("-Dmapreduce.job.queuename")) {
                    args.add(arg);
                }
            }
        }
        setHadoopArgs(args);
    }

    @JsonProperty("customer")
    public String getCustomer() {
        return customer;
    }

    @JsonProperty("customer")
    public void setCustomer(String customer) {
        this.customer = customer;
    }

    @JsonProperty("num_mappers")
    public int getNumMappers() {
        return numMappers;
    }

    @JsonProperty("num_mappers")
    public void setNumMappers(int numMappers) {
        this.numMappers = numMappers;
    }

    @JsonProperty("columns")
    public List<String> getColumnsToInclude() {
        return columnsToInclude;
    }

    @JsonProperty("columns")
    public void setColumnsToInclude(List<String> columnsToInclude) {
        this.columnsToInclude = columnsToInclude;
    }

    @JsonProperty("split_column")
    public String getSplitColumn() {
        return splitColumn;
    }

    @JsonProperty("split_column")
    public void setSplitColumn(String splitColumn) {
        this.splitColumn = splitColumn;
    }

    @JsonProperty("properties")
    public Properties getProperties() {
        return properties;
    }

    @JsonProperty("properties")
    public void setProperties(Properties properties) {
        this.properties = properties;
    }

    @JsonProperty("sync")
    public boolean isSync() {
        return sync;
    }

    @JsonProperty("sync")
    public void setSync(boolean sync) {
        this.sync = sync;
    }

    @JsonProperty("hadoop_args")
    public List<String> getHadoopArgs() {
        return hadoopArgs;
    }

    @JsonProperty("hadoop_args")
    public void setHadoopArgs(List<String> hadoopArgs) {
        this.hadoopArgs = hadoopArgs;
    }

    @JsonProperty("other_opts")
    public List<String> getOtherOptions() {
        return otherOptions;
    }

    @JsonProperty("other_opts")
    public void setOtherOptions(List<String> otherOptions) {
        this.otherOptions = otherOptions;
    }

    @JsonProperty("mode")
    public Mode getMode() {
        return mode;
    }

    @JsonProperty("mode")
    public void setMode(Mode mode) {
        this.mode = mode;
    }

    @Override
    public String toString() {
        return JsonUtils.serialize(this);
    }

    public String fullJobName() {
        return StringUtils.join(Arrays.asList(getCustomer(), "sqoop-import", dateTimeFormatter.print(new DateTime())), "-");
    }

    public static class Builder {

        private Mode mode = Mode.TABLE;
        private String table;
        private String query;
        private String targetDir;
        private DbCreds dbCreds;
        private String queue;
        private String customer;
        private String splitColumn;
        private List<String> columnsToInclude = new ArrayList<>();
        private Properties properties;
        private int numMappers = 0;
        private boolean sync = false;
        private List<String> hadoopArgs = new ArrayList<>();
        private List<String> otherOptions = new ArrayList<>(defaultOptions);

        public SqoopImporter build() {
            validate();
            SqoopImporter importer =  new SqoopImporter();
            importer.setMode(mode);
            importer.setTable(this.table);
            importer.setTargetDir(this.targetDir);
            importer.setDbCreds(this.dbCreds);
            importer.setQueue(this.queue);
            importer.setCustomer(this.customer);
            importer.setNumMappers(this.numMappers);
            importer.setColumnsToInclude(new ArrayList<>(this.columnsToInclude));
            importer.setSplitColumn(splitColumn);
            importer.setSync(this.sync);
            importer.setProperties(this.properties);
            importer.setQuery(query);

            Set<String> hadoopArgKeys = new HashSet<>();
            List<String> hadoopArgs = new ArrayList<>(importer.getHadoopArgs());
            for (String arg: hadoopArgs) {
                if (arg.contains("=")) {
                    hadoopArgKeys.add(arg.substring(0, arg.indexOf("=")));
                }
            }

            for (String arg: this.hadoopArgs) {
                String key = arg.substring(0, arg.indexOf("="));
                if (!hadoopArgKeys.contains(key)) {
                    hadoopArgKeys.add(key);
                    hadoopArgs.add(arg);
                }
            }

            for (String arg: defaultHadoopArgs) {
                String defaultKey = arg.substring(0, arg.indexOf("="));
                if (!hadoopArgKeys.contains(defaultKey)) {
                    hadoopArgKeys.add(defaultKey);
                    hadoopArgs.add(arg);
                }
            }

            importer.setHadoopArgs(new ArrayList<>(hadoopArgs));
            importer.setOtherOptions(new ArrayList<>(this.otherOptions));

            return importer;
        }

        public Builder setMode(Mode mode) {
            this.mode = mode;
            return this;
        }

        public Builder setTable(String table) {
            this.table = table;
            return this;
        }

        public Builder setQuery(String query) {
            this.query = query;
            return this;
        }

        public Builder setTargetDir(String targetDir) {
            this.targetDir = targetDir;
            return this;
        }

        public Builder setDbCreds(DbCreds dbCreds) {
            this.dbCreds = dbCreds;
            return this;
        }

        public Builder setQueue(String queue) {
            this.queue = queue;
            return this;
        }

        public Builder setCustomer(String customer) {
            this.customer = customer;
            return this;
        }

        public Builder setColumnsToInclude(List<String> columnsToInclude) {
            this.columnsToInclude = columnsToInclude;
            return this;
        }

        public Builder setSplitColumn(String splitColumn) {
            this.splitColumn = splitColumn;
            return this;
        }

        public Builder setProperties(Properties properties) {
            this.properties = properties;
            return this;
        }

        public Builder setNumMappers(int numMappers) {
            this.numMappers = numMappers;
            return this;
        }

        public Builder setSync(boolean sync) {
            this.sync = sync;
            return this;
        }

        public Builder addHadoopArg(String hadoopArg) {
            this.hadoopArgs.add(hadoopArg);
            return this;
        }

        public Builder addExtraOption(String option) {
            this.otherOptions.add(option);
            return this;
        }

        private void validate() {
            if (Mode.TABLE.equals(this.mode) && StringUtils.isEmpty(this.table)) {
                throw new IllegalStateException("Table name not provided when importing in TABLE mode.");
            }

            if (Mode.QUERY.equals(this.mode) && StringUtils.isEmpty(this.query)) {
                throw new IllegalStateException("Query not provided when importing in QUERY mode.");
            }

            if (StringUtils.isEmpty(splitColumn) && this.numMappers > 1) {
                throw new IllegalStateException("Split column is not specified while requesting more than 1 mappers.");
            }

        }
    }

    public enum Mode { TABLE, QUERY }

}
