package com.streever.hadoop.hive.money.honey;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.io.Text;

/**
 * Created by dstreev on 11/11/14.
 */

@Description(
        name = "standardize_money",
        value = "_FUNC_(text) - Returns converted $ by removing format characters from money",
        extended = "Examples:\n"
                + "  > SELECT _FUNC_('5,342,321.43') FROM src LIMIT 1;\n"
                + "  5342321.43\n"
)
public class StandardizeMoney extends UDF {
    /**
     * Returns converted $ by removing format characters from money.
     *
     * @return Returns converted $ by removing format characters from money
     *
     */

    private Text result = new Text();
    static final Log LOG = LogFactory.getLog(StandardizeMoney.class.getName());

    public Text evaluate(Text value) {
        if (value == null)
            return null;

        String working = ConvertMoney.convert(value.toString());
        result.set(working);

        return result;
    }

}

