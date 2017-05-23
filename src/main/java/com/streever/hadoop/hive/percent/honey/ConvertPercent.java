package com.streever.hadoop.hive.percent.honey;

/**
 * Created by dstreev on 2016-10-30.
 */
public class ConvertPercent {
    public static String convert(String value) {

        String working = value;
        if (value.contains("%")) {
            working = working.replaceAll("%","");
            Double wrkDbl = Double.parseDouble(working);
            wrkDbl = wrkDbl / 100;
            working = wrkDbl.toString();
        }
        return working;
    }
}
