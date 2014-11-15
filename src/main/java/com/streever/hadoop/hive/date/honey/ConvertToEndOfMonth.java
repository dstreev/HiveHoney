package com.streever.hadoop.hive.date.honey;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.io.Text;

import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;

/**
 * Created by dstreev on 11/11/14.
 */

@Description(
        name = "convert_to_date",
        value = "_FUNC_(text) - Returns the day at the end of the supplied month",
        extended = "Examples:\n"
                + "  > SELECT _FUNC_('MM/dd/yyyy', '12/31/2014') FROM src LIMIT 1;\n"
                + "  2014-12-31\n"
)
public class ConvertToEndOfMonth extends UDF {
    /**
     * returns a converted date from a specified date in declared format.
     *
     * @return converted date in format 'yyyy-MM-dd'
     *         string.
     */

    private Map<String, DateFormat> formatterMap = new TreeMap<String, DateFormat>();
    private Calendar cal = new GregorianCalendar();
    private SimpleDateFormat defaultFormatter = new SimpleDateFormat("yyyy-MM-dd");

    private Text result = new Text();
    static final Log LOG = LogFactory.getLog(ConvertToEndOfMonth.class.getName());

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

    public Text evaluate(Text sourceFormat, Text source) {
        DateFormat df = getFormatter(sourceFormat);
        if (df != null) {
            Date incoming = null;
            try {
                incoming = df.parse(source.toString());
            } catch (ParseException e) {
                LOG.error("Problem parsing date: " + source.toString() + " with source format: " + sourceFormat.toString(), e);
            }

            if (incoming != null) {
                cal.setTime(incoming);
                cal.add(Calendar.MONTH, 1);
                cal.set(Calendar.DAY_OF_MONTH, 1);
                cal.add(Calendar.DAY_OF_YEAR, -1);
                result.set(defaultFormatter.format(cal.getTime()));
            } else {
                result.set("");
            }
        } else {
            result.set("");
        }
        return result;
    }

}

