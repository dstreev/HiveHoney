package com.streever.hadoop.hive.record.honey;

/**
 * Created by dstreev on 11/14/14.
 */

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.io.Text;

import java.util.concurrent.atomic.AtomicLong;

@Description(
        name = "create a sequence number",
        value = "_FUNC_() - Returns and ever increasing sequence value for the control container",
        extended = "Examples:\n"
                + "  > SELECT _FUNC_() FROM src LIMIT 1;\n"
                + "  1\n")

public class Sequence extends UDF {
    static private AtomicLong seq = new AtomicLong(0);
    private Text result = new Text();
    public Text evaluate(Text text) {

        result.set(Long.toString(seq.incrementAndGet()));
        return result;

    }

}
