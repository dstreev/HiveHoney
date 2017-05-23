package com.streever.hadoop.hive.parse.honey;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.io.Text;

import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

/**
 * Created by dstreev on 08/01/2016.
 *
 * myudf(5,4,array('1','2'),historical_factors,",")
 * offset, group_size, array of element, field, new separator
 */

@Description(
        name = "rip_merge_build",
        value = "_FUNC_(int, int, array[int], text, text) - Returns convert date in format 'yyyy-MM-dd' from specified date",
        extended = "Examples:\n"
                + "  > SELECT _FUNC_() FROM src LIMIT 1;\n"
                + "  20141231\n"
)
public class RipMergeBuild extends UDF {
    /**
     * returns a converted date from a specified date in declared format.
     *
     * @return formatted string of source date.
     */


    public String evaluate(int offset, int group_size, List<Integer> array_of_elements, Text field, String delimiter, String separator) {

        String rtn = innerEval(offset,group_size,array_of_elements,field.toString(),delimiter,separator);

        return rtn;
    }

    public static String innerEval(int offset, int group_size, List<Integer> elements, String field, String delimiter, String separator) {

        StringBuilder sb = new StringBuilder();

        String[] base_elements = field.split(delimiter);

        int currLoc = offset;
        int group_remaining = group_size;
        while (currLoc < base_elements.length) {
            for (int i = 0;i < group_size;i++) {
                if (elements.contains(i)) {
                    sb.append(base_elements[currLoc+i]).append(separator);
                }
            }
            currLoc += group_size;
        }
        return sb.toString();
    }

}

