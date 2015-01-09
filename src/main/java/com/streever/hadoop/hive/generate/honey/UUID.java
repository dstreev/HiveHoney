package com.streever.hadoop.hive.generate.honey;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.io.Text;

import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Map;
import java.util.TreeMap;

/**
 * Created by dstreev on 11/11/14.
 */

@Description(
        name = "convert_to_date",
        value = "_FUNC_(text) - Returns a UUID",
        extended = "Examples:\n"
                + "  > SELECT _FUNC_(<field>) FROM src LIMIT 1;\n"
                + "  2014-12-31\n"
)
public class UUID extends UDF {
    /**
     * returns a UUID
     *
     * @return uuid
     */

    private Text result = new Text();
    static final Log LOG = LogFactory.getLog(UUID.class.getName());

    public Text evaluate(Text seedfield) {
        result.set(java.util.UUID.randomUUID().toString());
        return result;
    }

}

