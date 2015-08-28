package com.latticeengines.dataflow;

import java.io.File;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import org.apache.avro.Schema;
import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;

import cascading.avro.AvroScheme;
import cascading.flow.Flow;
import cascading.flow.FlowDef;
import cascading.flow.FlowStep;
import cascading.flow.FlowStepListener;
import cascading.flow.hadoop.HadoopFlowConnector;
import cascading.operation.aggregator.First;
import cascading.pipe.Each;
import cascading.pipe.Every;
import cascading.pipe.GroupBy;
import cascading.pipe.Merge;
import cascading.pipe.Pipe;
import cascading.property.AppProps;
import cascading.scheme.hadoop.TextDelimited;
import cascading.tap.SinkMode;
import cascading.tap.Tap;
import cascading.tap.hadoop.Lfs;
import cascading.tuple.Fields;

import com.latticeengines.common.exposed.util.AvroUtils;
import com.latticeengines.dataflow.runtime.cascading.AddNullColumns;
import com.latticeengines.scheduler.exposed.LedpQueueAssigner;

public class AvroRead {

    @SuppressWarnings({ "rawtypes", "unused", "unchecked" })
    public static void main(String[] args) throws Exception {
        FileUtils.deleteDirectory(new File("/tmp/AvroReadResults"));
        String lead = "file://"
                + ClassLoader.getSystemResource("com/latticeengines/dataflow/exposed/service/impl/Lead.avro").getPath();
        String file1 = "file://"
                + ClassLoader.getSystemResource("com/latticeengines/dataflow/exposed/service/impl/file1.avro").getPath();
        String file2 = "file://"
                + ClassLoader.getSystemResource("com/latticeengines/dataflow/exposed/service/impl/file2.avro").getPath();
        String file3 = "file://"
                + ClassLoader.getSystemResource("com/latticeengines/dataflow/exposed/service/impl/file3.avro").getPath();
        String opportunity = "file://"
                + ClassLoader.getSystemResource("com/latticeengines/dataflow/exposed/service/impl/Opportunity.avro")
                        .getPath();
        String contact = "file://"
                + ClassLoader.getSystemResource("com/latticeengines/dataflow/exposed/service/impl/Contact.avro")
                        .getPath();
        String wcPath = "/tmp/AvroReadResults";

        // Get the schema from a file
        Schema leadSchema = AvroUtils.getSchema(new Configuration(), new Path(lead));
        Schema opportunitySchema = AvroUtils.getSchema(new Configuration(), new Path(opportunity));
        Schema contactSchema = AvroUtils.getSchema(new Configuration(), new Path(contact));

        Properties properties = new Properties();
        properties.put("mapred.job.queue.name", LedpQueueAssigner.getPropDataQueueNameForSubmission());
        AppProps.setApplicationJarClass(properties, AvroRead.class);
        HadoopFlowConnector flowConnector = new HadoopFlowConnector(properties);
        //FlowConnector flowConnector = new LocalFlowConnector();

        AvroScheme leadScheme = new AvroScheme();
        Map<String, Tap> sources = new HashMap<>();
        Schema s = AvroUtils.getSchema(new Configuration(), new Path(file1));
        Tap<?, ?, ?> leadTap = new Lfs(new AvroScheme(), lead);
        Tap<?, ?, ?> file1Tap = new Lfs(new AvroScheme(), file1);
        Tap<?, ?, ?> file2Tap = new Lfs(new AvroScheme(), file2);
        Tap<?, ?, ?> file3Tap = new Lfs(new AvroScheme(), file3);
        Tap<?, ?, ?> opportunityTap = new Lfs(new AvroScheme(), opportunity);
        Tap<?, ?, ?> contactTap = new Lfs(new AvroScheme(), contact);
        Tap<?, ?, ?> sink = new Lfs(new TextDelimited(), wcPath, SinkMode.KEEP);
        
        sources.put("file1", file1Tap);
        sources.put("file2", file2Tap);
        sources.put("file3", file3Tap);
        //Pipe[] mergedPipes = Pipe.pipes(new Pipe("file1"), new Pipe("file2"), new Pipe("file3"));
        Pipe file2Pipe = new Each(new Pipe("file2"), new AddNullColumns(new Fields("Column3")), //
                new Fields("ID", "Column1", "Column2", "Column3", "Column4", "Column5", "LastUpdatedDate"));
        Pipe file3Pipe = new Each(new Pipe("file3"), new AddNullColumns(new Fields("Column1")), //
                new Fields("ID", "Column1", "Column2", "Column3", "Column4", "Column5", "LastUpdatedDate"));
        
        Pipe merge = new Merge(new Pipe("file1"), file2Pipe, file3Pipe);
        Fields sortFields = new Fields("LastUpdatedDate");
        sortFields.setComparator("LastUpdatedDate", Collections.reverseOrder());
        Pipe groupby = new GroupBy(merge, new Fields("ID"), sortFields);
        Every first = new Every(groupby, Fields.ALL, new First(), Fields.RESULTS);
        
        /*
        //sources.put("oppty", opportunityTap);
        sources.put("contact", contactTap);

        List<String> fieldNames = new ArrayList<>();
        Set<String> seenFieldNames = new HashSet<>();

        List<Class<?>> types = new ArrayList<>();
        for (Field field : leadSchema.getFields()) {
            String name = field.name();
            if (seenFieldNames.contains(name)) {
                name = "lead$" + name;
            }
            fieldNames.add(name);
            Type avroType = field.schema().getTypes().get(0).getType();
            types.add(AvroUtils.getJavaType(avroType));
            seenFieldNames.add(name);
        }

        for (Field field : contactSchema.getFields()) {
            String name = field.name();
            if (seenFieldNames.contains(name)) {
                name = "contact$" + name;
            }
            Type avroType = field.schema().getTypes().get(0).getType();
            types.add(AvroUtils.getJavaType(avroType));
            fieldNames.add(name);
            seenFieldNames.add(name);
        }
        String[] declaredFields = new String[fieldNames.size()];
        fieldNames.toArray(declaredFields);
        Fields e1 = new Fields("Email");
        e1.setComparators(new NullNotEquivalentComparator());
        Fields e2 = new Fields("Email");
        e2.setComparators(new NullNotEquivalentComparator());
        Pipe join = new CoGroup(new Pipe("lead"), //
                e1, //
                new Pipe("contact"), //
                e2, //
                new Fields(declaredFields), //
                new InnerJoin());

        DelimitedParser parser = new DelimitedParser(";", //
                "\"", //
                null, //
                false, //
                true, //
                null, //
                null);
        TextDelimited scheme = new TextDelimited(true, parser);
        Checkpoint c1 = new Checkpoint("ckpt1", join);
        Tap<?, ?, ?> c1Tap = new Lfs(new SequenceFile(Fields.UNKNOWN), //
                "/tmp/ckpt1", SinkMode.KEEP);

        ExpressionFunction function = new ExpressionFunction(new Fields("Domain"), //
                "Email != null ? Email.substring(Email.indexOf(\"@\") + 1) : \"\"", //
                new String[] { "Email" }, new Class[] { String.class });
        Each each = new Each(c1, new Fields("Email"), function, Fields.ALL);

        Checkpoint c2 = new Checkpoint("ckpt2", each);
        Tap<?, ?, ?> c2Tap = new Lfs(new SequenceFile(Fields.UNKNOWN), //
                "/tmp/ckpt2", SinkMode.KEEP);

        ExpressionFunction domainFunction = new ExpressionFunction(new Fields("IsDomain"), //
                "Domain == null ? false : Domain.contains(\".com\") || Domain.contains(\"www\")", //
                new String[] { "Domain" }, new Class[] { String.class });

        each = new Each(c2, new Fields("Domain"), domainFunction, Fields.ALL);

        Checkpoint c3 = new Checkpoint("ckpt3", each);
        Tap<?, ?, ?> c3Tap = new Lfs(new SequenceFile(Fields.UNKNOWN), //
                "/tmp/ckpt3", SinkMode.KEEP);

        Pipe grpby = new GroupBy(c3, new Fields("Domain"), new Fields("FirstName"));

        grpby = new Every(grpby, Fields.ALL, new Last(), Fields.RESULTS);

        grpby = new Every(grpby, new Fields("AnnualRevenue"), new MaxValue(new Fields("MaxRevenue")), Fields.ALL);
        grpby = new Every(grpby, new Fields("NumberOfEmployees"), new Sum(new Fields("TotalEmployees")), Fields.ALL);

        Checkpoint c4 = new Checkpoint("ckpt4", grpby);
        Tap<?, ?, ?> c4Tap = new Lfs(new SequenceFile(Fields.UNKNOWN), //
                "/tmp/ckpt4", SinkMode.KEEP);

        each = new Each(c4, new Fields("Domain", "MaxRevenue", "TotalEmployees"), new AddMD5Hash(new Fields("PropDataHash")), Fields.ALL);

        each = new Each(each, Fields.GROUP, new JythonFunction("com/latticeengines/domain/exposed/transforms/python/encoder.py", "transform", Integer.class, new Fields("Domain"), new Fields("DomainAsInt")), Fields.ALL);
/*
        ExpressionFilter filter = new ExpressionFilter(
                "(Email == null || Email.trim().isEmpty()) && (contact$Email == null || contact$Email.trim().isEmpty())", //
                new String[] { "Email", "contact$Email" }, //
                new Class<?>[] { String.class, String.class });
        //Not not = new Not(filter);

        join = new Each(join, new Fields("Email", "contact$Email"), filter);

        //join = new Each(join, new Fields("Domain", "Company", "City", "Country"), new AddMD5Hash(new Fields("PropDataHash")), Fields.ALL);

        //join = new GroupBy(join, new Fields("Domain", "Company", "City", "Country", "PropDataHash", "IsDomain"));

        //join = new Every(join, Fields.ALL, new Last(), Fields.GROUP);
         *

        FlowDef flowDef = FlowDef.flowDef().setName("wc") //
                .addSources(sources) //
                .addTailSink(each, sink) //
                .addCheckpoint(c1, c1Tap) //
                .addCheckpoint(c2, c2Tap) //
                .addCheckpoint(c3, c3Tap) //
                .addCheckpoint(c4, c4Tap); //
         */
        FlowDef flowDef = FlowDef.flowDef().setName("wc") //
                .addSources(sources) //
                .addTailSink(first, sink); //
        // write a DOT file and run the flow
        Flow<?> flow = flowConnector.connect(flowDef);
        flow.writeDOT("dot/wcr.dot");
        flow.addStepListener(new DefaultFlowStepListener());
        flow.complete();
    }

    @SuppressWarnings("rawtypes")
    static class DefaultFlowStepListener implements FlowStepListener {

        @Override
        public void onStepStarting(FlowStep flowStep) {
            System.out.println("Starting " + flowStep.getName());
        }

        @Override
        public void onStepStopping(FlowStep flowStep) {
            System.out.println("Stopping " + flowStep.getName());
        }

        @Override
        public void onStepRunning(FlowStep flowStep) {
            System.out.println("Running " + flowStep.getName());
        }

        @Override
        public void onStepCompleted(FlowStep flowStep) {
            System.out.println("Completed " + flowStep.getName());
        }

        @Override
        public boolean onStepThrowable(FlowStep flowStep, Throwable throwable) {
            throwable.printStackTrace();
            return false;
        }

    }

}

