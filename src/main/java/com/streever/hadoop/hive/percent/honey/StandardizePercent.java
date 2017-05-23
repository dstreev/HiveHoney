package com.streever.hadoop.hive.percent.honey;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.io.Text;

/**
 * Created by dstreev on 11/11/14.
 */

@Description(
        name = "standardize_percent",
        value = "_FUNC_(text) - Returns percent from formatted percent",
        extended = "Examples:\n"
                + "  > SELECT _FUNC_('5,342,321.43') FROM src LIMIT 1;\n"
                + "  5342321.43\n"
)
public class StandardizePercent extends UDF {
    /**
     * Returns converted $ by removing ',' from money.
     *
     * @return Returns converted $ by removing ',' from money
     *
     */

    private Text result = new Text();
    static final Log LOG = LogFactory.getLog(StandardizePercent.class.getName());

    public Text evaluate(Text value) {
        // Look for trailing "%" and remove, then divide value by 100.
        if (value == null)
            return null;

        String working = ConvertPercent.convert(value.toString());
        result.set(working);

        return result;
    }

}

