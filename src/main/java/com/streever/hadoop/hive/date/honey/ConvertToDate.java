package com.streever.hadoop.hive.date.honey;

import java.text.DateFormat;
import java.text.ParseException;
import java.util.Date;

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.io.Text;
import java.text.SimpleDateFormat;
import java.util.Map;
import java.util.TreeMap;


import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * Created by dstreev on 11/11/14.
 */

@Description(
        name = "convert_to_date",
        value = "_FUNC_(text, text) - Returns convert date in format 'yyyy-MM-dd' from specified date",
        extended = "Examples:\n"
                + "  > SELECT _FUNC_('MM/dd/yyyy', '12/31/2014') FROM src LIMIT 1;\n"
                + "  2014-12-31\n"
)
public class ConvertToDate extends UDF {
    /**
     * returns a converted date from a specified date in declared format.
     *
     * @return converted date in format 'yyyy-MM-dd'
     *         string.
     */

    private Map<String, DateFormat> formatterMap = new TreeMap<String, DateFormat>();

    private SimpleDateFormat defaultFormatter = new SimpleDateFormat("yyyy-MM-dd");

    private Text result = new Text();
    static final Log LOG = LogFactory.getLog(ConvertToDate.class.getName());

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

