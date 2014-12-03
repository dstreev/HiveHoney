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
        name = "long_to_ip",
        value = "_FUNC_(text) - Returns Long version of IpAddress",
        extended = "Examples:\n"
                + "  > SELECT _FUNC_(167778091) FROM src LIMIT 1;\n"
                + "  10.0.23.43\n"
)
public class LongToIp extends UDF {
    /**
     * returns Returns Long version of IpAddress.
     *
     * @return Long version of IpAddress.
     */

    private Text result = new Text();
    static final Log LOG = LogFactory.getLog(LongToIp.class.getName());

    public Text evaluate(Text ipAddress) {
        if (ipAddress != null) {
            result.set(IPConverter.convertFromLong(Long.parseLong(ipAddress.toString())));
        } else {
            result.set("");
        }
        return result;
    }

}

