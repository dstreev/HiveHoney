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
        name = "convert_to_timestamp",
        value = "_FUNC_(text, text) - Returns converted timestamp in format 'yyyy-MM-dd HH:mm:ss.SSS' from specified date in a format",
        extended = "Examples:\n"
                + "  > SELECT _FUNC_('MM/dd/yyyy h:mm:ss a', '12/31/2014 9:02:12 PM') FROM src LIMIT 1;\n"
                + "  2014-12-31 21:02:12.000\n"
)
public class ConvertToTimestamp extends UDF {
    /**
     * returns a converted date from a specified date in declared format.
     *
     * @return converted timestamp in format 'yyyy-MM-dd HH:mm:ss.SSS'
     *         string.
     */

    private Map<String, DateFormat> formatterMap = new TreeMap<String, DateFormat>();

    private SimpleDateFormat defaultFormatter = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");

    private Text result = new Text();
    static final Log LOG = LogFactory.getLog(ConvertToTimestamp.class.getName());

    private DateFormat getFormatter(Text format) {
        DateFormat rtn = formatterMap.get(format.toString());
        if (rtn == null) {
            try {
                rtn = new SimpleDateFormat(format.toString());
                formatterMap.put(format.toString(), rtn);
            } catch (Exception e) {
                // Log an error.
                LOG.error("Problem with date format: " + format.toString(),e);
            }

        }
        return rtn;
    }

    public Text evaluate(Text incomingFormat, Text source) {
        DateFormat df = getFormatter(incomingFormat);
        if (df != null) {
            Date incoming = null;
            try {
                incoming = df.parse(source.toString());
            } catch (ParseException e) {
                LOG.error("Problem parsing date: " + source.toString() + " with source format: " + incomingFormat.toString(), e);
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

