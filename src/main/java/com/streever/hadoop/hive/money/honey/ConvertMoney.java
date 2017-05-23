package com.streever.hadoop.hive.money.honey;

/**
 * Created by dstreev on 2016-10-30.
 */
public class ConvertMoney {
    public static String convert(String value) {
        String working = value.replaceAll(",|'","");
        working = working.replaceAll("\\$","");
        if (working.contains("(")) {
            working = working.replaceAll("\\(|\\)","");
            working = "-" + working;
        }
        if (working.contains("M")) {
            working = working.replaceAll("M","000000");
        }
//        if (without.startsWith("(")) {
//            // This means it is surrounded by parentheses and is negative.
//            StringBuilder sb = new StringBuilder("-");
//            sb.append(without.substring(1,without.length()-1));
//            result.set(sb.toString());
//        } else {
//            result.set(without);
//        }

        return working;
    }
}
