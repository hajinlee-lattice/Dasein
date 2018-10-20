package com.latticeengines.domain.exposed.dataplatform;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
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
public class SqoopExporter {

    private static List<String> defaultHadoopArgs = Arrays.asList("-Dmapreduce.task.timeout=600000",
            "-Dmapreduce.job.running.map.limit=32", "-Dmapreduce.tasktracker.map.tasks.maximum=32");

    private static DateTimeFormatter dateTimeFormatter = ISODateTimeFormat.dateTime();

    @JsonProperty("table")
    private String table;

    @JsonProperty("source_dir")
    private String sourceDir;

    @JsonProperty("db_creds")
    private DbCreds dbCreds;

    @JsonProperty("queue")
    private String queue;

    @JsonProperty("customer")
    private String customer;

    @JsonProperty("num_mappers")
    private int numMappers;

    @JsonProperty("java_column_type_mappings")
    private String javaColumnTypeMappings;

    @JsonProperty("export_columns")
    private List<String> exportColumns;

    @JsonProperty("sync")
    private boolean sync;

    @JsonProperty("hadoop_args")
    private List<String> hadoopArgs;

    @JsonProperty("other_opts")
    private List<String> otherOptions;

    public String getTable() {
        return table;
    }

    public void setTable(String table) {
        this.table = table;
    }

    public String getSourceDir() {
        return sourceDir;
    }

    public void setSourceDir(String sourceDir) {
        this.sourceDir = sourceDir;
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

    public String getJavaColumnTypeMappings() {
        return javaColumnTypeMappings;
    }

    public void setJavaColumnTypeMappings(String javaColumnTypeMappings) {
        this.javaColumnTypeMappings = javaColumnTypeMappings;
    }

    public List<String> getExportColumns() {
        return exportColumns;
    }

    public void setExportColumns(List<String> exportColumns) {
        this.exportColumns = exportColumns;
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

    @Override
    public String toString() {
        return JsonUtils.serialize(this);
    }

    public String fullJobName() {
        return StringUtils.join(Arrays.asList(getCustomer(), "sqoop-export",
                dateTimeFormatter.print(new DateTime())), "-");
    }

    public static class Builder {

        private String table;
        private String sourceDir;
        private DbCreds dbCreds;
        private String queue;
        private String customer;
        private int numMappers = 0;
        private String javaColumnTypeMappings;
        private List<String> exportColumns = new ArrayList<>();
        private boolean sync = false;
        private List<String> hadoopArgs = new ArrayList<>();
        private List<String> otherOptions = new ArrayList<>();

        public SqoopExporter build() {
            SqoopExporter exporter = new SqoopExporter();
            exporter.setTable(this.table);
            exporter.setSourceDir(this.sourceDir);
            exporter.setDbCreds(this.dbCreds);
            exporter.setQueue(this.queue);
            exporter.setCustomer(this.customer);
            exporter.setNumMappers(this.numMappers);
            exporter.setJavaColumnTypeMappings(this.javaColumnTypeMappings);
            exporter.setExportColumns(new ArrayList<>(this.exportColumns));
            exporter.setSync(this.sync);

            Set<String> hadoopArgKeys = new HashSet<>();
            List<String> hadoopArgs = new ArrayList<>(exporter.getHadoopArgs());
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

            exporter.setHadoopArgs(new ArrayList<>(hadoopArgs));
            exporter.setOtherOptions(new ArrayList<>(this.otherOptions));

            return exporter;
        }

        public Builder setTable(String table) {
            this.table = table;
            return this;
        }

        public Builder setSourceDir(String sourceDir) {
            this.sourceDir = sourceDir;
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

        public Builder setNumMappers(int numMappers) {
            this.numMappers = numMappers;
            return this;
        }

        public Builder setJavaColumnTypeMappings(String javaColumnTypeMappings) {
            this.javaColumnTypeMappings = javaColumnTypeMappings;
            return this;
        }

        public Builder setExportColumns(List<String> exportColumns) {
            this.exportColumns = exportColumns;
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
    }

}
