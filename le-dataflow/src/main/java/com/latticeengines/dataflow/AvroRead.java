package com.latticeengines.dataflow;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import org.apache.avro.Schema;
import org.apache.avro.Schema.Field;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;

import cascading.avro.AvroScheme;
import cascading.flow.Flow;
import cascading.flow.FlowDef;
import cascading.flow.FlowStep;
import cascading.flow.FlowStepListener;
import cascading.flow.hadoop.HadoopFlowConnector;
import cascading.pipe.CoGroup;
import cascading.pipe.Pipe;
import cascading.pipe.joiner.OuterJoin;
import cascading.property.AppProps;
import cascading.scheme.hadoop.TextDelimited;
import cascading.tap.Tap;
import cascading.tap.hadoop.Lfs;
import cascading.tuple.Fields;
import cascading.util.NullNotEquivalentComparator;

import com.latticeengines.common.exposed.util.AvroUtils;

public class AvroRead {

    @SuppressWarnings("deprecation")
    public static void main(String[] args) throws Exception {
        String lead = "file://"
                + ClassLoader.getSystemResource("com/latticeengines/dataflow/exposed/service/impl/Lead.avro").getPath();
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
        properties.put("mapred.job.queue.name", "Priority0.MapReduce.0");
        AppProps.setApplicationJarClass(properties, AvroRead.class);
        HadoopFlowConnector flowConnector = new HadoopFlowConnector(properties);
        // FlowConnector flowConnector = new LocalFlowConnector();

        Map<String, Tap> sources = new HashMap<>();
        Tap<?, ?, ?> leadTap = new Lfs(new AvroScheme(), lead);
        Tap<?, ?, ?> opportunityTap = new Lfs(new AvroScheme(), opportunity);
        Tap<?, ?, ?> contactTap = new Lfs(new AvroScheme(), contact);
        Tap<?, ?, ?> sink = new Lfs(new TextDelimited(new Fields("Id", "Email", "contact$Email")), wcPath, true);
        sources.put("lead", leadTap);
        //sources.put("oppty", opportunityTap);
        sources.put("contact", contactTap);

        List<String> fieldNames = new ArrayList<>();
        Set<String> seenFieldNames = new HashSet<>();

        for (Field field : leadSchema.getFields()) {
            String name = field.name();
            if (seenFieldNames.contains(name)) {
                name = "lead$" + name;
            }
            fieldNames.add(name);
            seenFieldNames.add(name);
        }

        for (Field field : contactSchema.getFields()) {
            String name = field.name();
            if (seenFieldNames.contains(name)) {
                name = "contact$" + name;
            }
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
                new OuterJoin());
        /*
        ExpressionFunction function = new ExpressionFunction(new Fields("Domain"), //
                "Email.substring(Email.indexOf('@') + 1)", //
                new String[] { "Email" }, new Class[] { String.class });

        join = new Each(join, new Fields("Email"), function, Fields.ALL);

        ExpressionFunction domainFunction = new ExpressionFunction(new Fields("IsDomain"), //
                "Domain.contains(\".com\") || Domain.contains(\"www\")", //
                new String[] { "Domain" }, new Class[] { String.class });

        join = new Each(join, new Fields("Domain"), domainFunction, Fields.ALL);

        //join = new GroupBy(join, new Fields("Domain"), new Fields("FirstName"));

        // join = new Every(join, Fields.ALL, new Last(), Fields.RESULTS);*/
        /*
        join = new Every(join, new Fields("AnnualRevenue"), new MaxValue(new Fields("MaxRevenue")), Fields.ALL);
        join = new Every(join, new Fields("NumberOfEmployees"), new Sum(new Fields("TotalEmployees")), Fields.ALL);
    */
/*
        ExpressionFilter filter = new ExpressionFilter(
                "(Email == null || Email.trim().isEmpty()) && (contact$Email == null || contact$Email.trim().isEmpty())", //
                new String[] { "Email", "contact$Email" }, //
                new Class<?>[] { String.class, String.class }); 
        Not not = new Not(filter);
               
        join = new Each(join, new Fields("Email", "contact$Email"), filter);
*/
        //join = new Each(join, new Fields("Domain", "Company", "City", "Country"), new AddMD5Hash(new Fields("PropDataHash")), Fields.ALL);

        //join = new GroupBy(join, new Fields("Domain", "Company", "City", "Country", "PropDataHash", "IsDomain"));
        
        //join = new Every(join, Fields.ALL, new Last(), Fields.GROUP);
        FlowDef flowDef = FlowDef.flowDef().setName("wc") //
                .addSources(sources) //
                .addTailSink(join, sink);

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
