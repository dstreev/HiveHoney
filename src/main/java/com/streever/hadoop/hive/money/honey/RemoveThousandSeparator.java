package com.streever.hadoop.hive.money.honey;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.io.Text;

import java.text.DateFormat;
import java.text.ParseException;
import java.util.Date;

/**
 * Created by dstreev on 11/11/14.
 */

@Description(
        name = "remove_thousand_separator",
        value = "_FUNC_(text) - Returns converted $ by removing ',' from money",
        extended = "Examples:\n"
                + "  > SELECT _FUNC_('5,342,321.43') FROM src LIMIT 1;\n"
                + "  5342321.43\n"
)
public class RemoveThousandSeparator extends UDF {
    /**
     * Returns converted $ by removing ',' from money.
     *
     * @return Returns converted $ by removing ',' from money
     *
     */

    private Text result = new Text();
    static final Log LOG = LogFactory.getLog(RemoveThousandSeparator.class.getName());

    public Text evaluate(Text value) {
        String without = value.toString().replaceAll(",|'","");
        result.set(without);
        return result;
    }

}

