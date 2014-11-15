package com.streever.hadoop.hive.generate.honey;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.exec.*;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFEvaluator;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFUtils;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.StringObjectInspector;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.util.*;

/**
 * Created by dstreev on 11/15/14.
 *
 * Using a file in HDFS, read and build a set of items that can be random picked from to insert into a
 *   field in hive.
 *
 */

@Description(
        name = "convert_to_date",
        value = "_FUNC_(text, text) - Returns convert date in format 'yyyy-MM-dd' from specified date",
        extended = "Examples:\n"
                + "  > SELECT _FUNC_('MM/dd/yyyy', '12/31/2014') FROM src LIMIT 1;\n"
                + "  2014-12-31\n"
)

public class SeedFromList extends GenericUDF {

    private static final String HIVE_HONEY_SEED_FILE = "hive.honey.seed.file";

    private List items = new ArrayList<String>();
    private Random random = new Random();
    private Text result = new Text();

    private GenericUDFUtils.ReturnObjectInspectorResolver returnOIResolver;
    private ObjectInspector[] argumentOIs;


    @Override
    public ObjectInspector initialize(ObjectInspector[] arguments) throws UDFArgumentException {
        argumentOIs = arguments;

        if (arguments.length != 1) {
            throw new UDFArgumentLengthException(
                    "The operator 'NVL' accepts 1 arguments.");
        }
        returnOIResolver = new GenericUDFUtils.ReturnObjectInspectorResolver(true);
        if (!(returnOIResolver.update(arguments[0]))) {
            throw new UDFArgumentTypeException(2,
                    "The 1st and 2nd args of function NLV should have the same type, "
                            + "but they are different: \"" + arguments[0].getTypeName()
                            + "\" and \"" + arguments[1].getTypeName() + "\"");
        }
        return returnOIResolver.get();
    }

    public void configure(MapredContext mapredContext) {
        JobConf conf = mapredContext.getJobConf();
        String seedFile = conf.get(HIVE_HONEY_SEED_FILE);

        try{
            Path pt=new Path(seedFile);
            FileSystem fs = FileSystem.get(new Configuration());
            BufferedReader br=new BufferedReader(new InputStreamReader(fs.open(pt)));
            String line;
            line=br.readLine();
            while (line != null){
                items.add(line);
                line=br.readLine();
            }
        }catch(Exception e){
            System.out.println("Couldn't Load..");
            throw new RuntimeException("Configuration: " + HIVE_HONEY_SEED_FILE + " has not been set or doesn't point to a valid file");
        }

    }

    @Override
    public Object evaluate(DeferredObject[] arguments) throws HiveException {

//        Object retVal = returnOIResolver.convertIfNecessary(arguments[0].get(),
//                argumentOIs[0]);
//        if (retVal == null ){
//            retVal = returnOIResolver.convertIfNecessary(arguments[1].get(),
//                    argumentOIs[1]);
//        }
//        return retVal;


        result.set(items.get(random.nextInt(items.size())).toString());
        return result;

    }

    @Override
    public String getDisplayString(String[] strings) {
        return "something";
    }


}