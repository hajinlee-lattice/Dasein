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

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.modeling.DbCreds;

@JsonIgnoreProperties(ignoreUnknown = true)
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonAutoDetect(fieldVisibility = JsonAutoDetect.Visibility.NONE, getterVisibility = JsonAutoDetect.Visibility.NONE)
public class SqoopImporter {

    private static List<String> defaultHadoopArgs = Arrays.asList( //
            "-Dmapreduce.task.timeout=600000", //
            "-Dmapreduce.job.running.map.limit=64", //
            "-Dmapreduce.tasktracker.map.tasks.maximum=64");

    private static List<String> defaultOptions = Arrays.asList( //
            "--relaxed-isolation", //
            "--as-avrodatafile", //
            "--compress");

    private static DateTimeFormatter dateTimeFormatter = ISODateTimeFormat.dateTime();

    @JsonProperty("table")
    private String table;

    @JsonProperty("query")
    private String query;

    @JsonProperty("target_dir")
    private String targetDir;

    @JsonProperty("db_creds")
    private DbCreds dbCreds;

    @JsonProperty("queue")
    private String queue;

    @JsonProperty("customer")
    private String customer;

    @JsonProperty("num_mappers")
    private int numMappers;

    @JsonProperty("split_column")
    private String splitColumn;

    @JsonProperty("columns")
    private List<String> columnsToInclude;

    @JsonProperty("sync")
    private boolean sync;

    @JsonProperty("properties")
    private Properties properties;

    @JsonProperty("hadoop_args")
    private List<String> hadoopArgs;

    @JsonProperty("other_opts")
    private List<String> otherOptions;

    @JsonProperty("mode")
    private Mode mode;

    @JsonProperty("emr_cluster")
    private String emrCluster;

    public String getTable() {
        return table;
    }

    public void setTable(String table) {
        this.table = table;
    }

    public String getQuery() {
        return query;
    }

    public void setQuery(String query) {
        this.query = query;
    }

    public String getTargetDir() {
        return targetDir;
    }

    public void setTargetDir(String targetDir) {
        this.targetDir = targetDir;
    }

    public DbCreds getDbCreds() {
        return dbCreds;
    }

    public void setDbCreds(DbCreds dbCreds) {
        this.dbCreds = dbCreds;
    }

    public String getQueue() {
        return queue;
    }

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

    public String getCustomer() {
        return customer;
    }

    public void setCustomer(String customer) {
        this.customer = customer;
    }

    public int getNumMappers() {
        return numMappers;
    }

    public void setNumMappers(int numMappers) {
        this.numMappers = numMappers;
    }

    public List<String> getColumnsToInclude() {
        return columnsToInclude;
    }

    public void setColumnsToInclude(List<String> columnsToInclude) {
        this.columnsToInclude = columnsToInclude;
    }

    public String getSplitColumn() {
        return splitColumn;
    }

    public void setSplitColumn(String splitColumn) {
        this.splitColumn = splitColumn;
    }

    public Properties getProperties() {
        return properties;
    }

    public void setProperties(Properties properties) {
        this.properties = properties;
    }

    public boolean isSync() {
        return sync;
    }

    public void setSync(boolean sync) {
        this.sync = sync;
    }

    public List<String> getHadoopArgs() {
        return hadoopArgs;
    }

    public void setHadoopArgs(List<String> hadoopArgs) {
        this.hadoopArgs = hadoopArgs;
    }

    public List<String> getOtherOptions() {
        return otherOptions;
    }

    public void setOtherOptions(List<String> otherOptions) {
        this.otherOptions = otherOptions;
    }

    public Mode getMode() {
        return mode;
    }

    public void setMode(Mode mode) {
        this.mode = mode;
    }

    @Override
    public String toString() {
        return JsonUtils.serialize(this);
    }

    public String fullJobName() {
        return StringUtils.join(Arrays.asList(getCustomer(), "sqoop-import",
                dateTimeFormatter.print(new DateTime())), "-");
    }

    public enum Mode {
        TABLE, QUERY
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
            SqoopImporter importer = new SqoopImporter();
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
            for (String arg : hadoopArgs) {
                if (arg.contains("=")) {
                    hadoopArgKeys.add(arg.substring(0, arg.indexOf("=")));
                }
            }

            for (String arg : this.hadoopArgs) {
                String key = arg.substring(0, arg.indexOf("="));
                if (!hadoopArgKeys.contains(key)) {
                    hadoopArgKeys.add(key);
                    hadoopArgs.add(arg);
                }
            }

            for (String arg : defaultHadoopArgs) {
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
                throw new IllegalStateException(
                        "Table name not provided when importing in TABLE mode.");
            }

            if (Mode.QUERY.equals(this.mode) && StringUtils.isEmpty(this.query)) {
                throw new IllegalStateException("Query not provided when importing in QUERY mode.");
            }

            if (StringUtils.isEmpty(splitColumn) && this.numMappers > 1) {
                throw new IllegalStateException(
                        "Split column is not specified while requesting more than 1 mappers.");
            }

        }
    }

}
