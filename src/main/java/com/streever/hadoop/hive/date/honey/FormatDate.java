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
 * Created by dstreev on 09/25/2015.
 */

@Description(
        name = "format_date",
        value = "_FUNC_(text, text) - Returns convert date in format 'yyyy-MM-dd' from specified date",
        extended = "Examples:\n"
                + "  > SELECT _FUNC_('yyyyMMdd', '2015-08-14') FROM src LIMIT 1;\n"
                + "  20141231\n"
)
public class FormatDate extends UDF {
    /**
     * returns a converted date from a specified date in declared format.
     *
     * @return formatted string of source date.
     */

    private Map<String, DateFormat> formatterMap = new TreeMap<String, DateFormat>();

    private SimpleDateFormat defaultFormatter = new SimpleDateFormat("yyyy-MM-dd");

    private Text result = new Text();
    static final Log LOG = LogFactory.getLog(FormatDate.class.getName());

    private DateFormat getFormatter(Text format) {
        DateFormat rtn = formatterMap.get(format.toString());
        if (rtn == null) {
            try {
                rtn = new SimpleDateFormat(format.toString());
                formatterMap.put(format.toString(), rtn);
            } catch (Exception e) {
                // Log an error.
                LOG.error("Problem with date format: " + format.toString(),e);
                throw new RuntimeException("Problem with date format: " + format.toString());
            }

        }
        return rtn;
    }

    public Text evaluate(Text desiredFormat, Text source) {
        DateFormat df = getFormatter(desiredFormat);
        if (df != null) {
            Date incoming = null;
            if (source != null && source != null) {
                try {
                    incoming = defaultFormatter.parse(source.toString());
                } catch (ParseException e) {
                    LOG.error("Problem parsing date: " + source.toString() + " with source format: " + defaultFormatter.toPattern(), e);
                }

                if (incoming != null) {
                    result.set(df.format(incoming));
                } else {
                    result.set("");
                }
            } else {
                result.set("");
            }
        } else {
            result.set("");
        }
        return result;
    }

}

