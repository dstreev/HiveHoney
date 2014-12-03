package com.streever.hadoop.hive.ip.honey;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.io.Text;

/**
 * Created by dstreev on 11/11/14.
 */

@Description(
        name = "ip_to_long",
        value = "_FUNC_(text) - Returns Long version of IpAddress",
        extended = "Examples:\n"
                + "  > SELECT _FUNC_('10.0.23.43') FROM src LIMIT 1;\n"
                + "  167778091\n"
)
public class IpToLong extends UDF {
    /**
     * returns Returns Long version of IpAddress.
     *
     * @return Long version of IpAddress.
     */

    private Text result = new Text();
    static final Log LOG = LogFactory.getLog(IpToLong.class.getName());

    public Text evaluate(Text ipAddress) {
        if (ipAddress != null) {
            result.set(IPConverter.convertFromString(ipAddress.toString()).toString());
        } else {
            result.set("");
        }
        return result;
    }

}

