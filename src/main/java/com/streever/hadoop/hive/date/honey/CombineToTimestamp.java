package com.streever.hadoop.hive.date.honey;

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
        name = "combine_to_timestamp",
        value = "_FUNC_(text, text, text, text) - Returns converted timestamp in format 'yyyy-MM-dd HH:mm:ss:' from specified date and time elements",
        extended = "Examples:\n"
                + "  > SELECT _FUNC_('MM/dd/yyyy', '12/31/2014', 'H:mm:ss AM', '9:02:21') FROM src LIMIT 1;\n"
                + "  2014-12-31 21:02:21.000\n"
)
public class CombineToTimestamp extends UDF {
    /**
     * returns a converted date from a specified date in declared format.
     *
     * @return converted date in format 'yyyy-mm-dd'
     *         string.
     */

    private Map<String, DateFormat> formatterMap = new TreeMap<String, DateFormat>();

    private SimpleDateFormat defaultFormatter = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");

    private Text result = new Text();
    static final Log LOG = LogFactory.getLog(CombineToTimestamp.class.getName());

    private DateFormat getFormatter(Text dateFormat, Text timeFormat) {
        String dtFormat = dateFormat.toString() + " " + timeFormat.toString();
        DateFormat rtn = formatterMap.get(dtFormat);
        if (rtn == null) {
            try {
                rtn = new SimpleDateFormat(dtFormat);
                formatterMap.put(dtFormat, rtn);
            } catch (Exception e) {
                // Log an error.
                LOG.error("Problem with date format: " + dtFormat,e);
                throw new RuntimeException(e);
            }

        }
        return rtn;
    }

    public Text evaluate(Text dateFormat, Text dateSource, Text timeFormat, Text timeSource) {
        DateFormat df = getFormatter(dateFormat, timeFormat);
        if (df != null) {
            Date incoming = null;
            String inboundTS = null;
            try {
                inboundTS = dateSource.toString() + " " + timeSource.toString();
                incoming = df.parse(inboundTS);
            } catch (ParseException e) {
                LOG.error("Problem parsing date: " + inboundTS + " with source format: " + inboundTS, e);
            }

            if (incoming != null) {
                result.set(defaultFormatter.format(incoming));
            } else {
                result.set("");
            }
        } else {
            result.set("");
        }
        return result;
    }

}

