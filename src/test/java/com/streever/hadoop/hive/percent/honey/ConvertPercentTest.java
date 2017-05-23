package com.streever.hadoop.hive.percent.honey;

import org.junit.Test;

import static org.junit.Assert.assertEquals;

/**
 * Created by dstreev on 2016-10-30.
 */
public class ConvertPercentTest {

    @Test
    public void ConvertPercent001() {
        String test01 = "2.50%";
        String test01Converted = ConvertPercent.convert(test01);

        System.out.println(test01Converted);

        assertEquals("0.025", test01Converted);

    }

    @Test
    public void ConvertPercent002() {
        String test01 = "-0.25%";
        String test01Converted = ConvertPercent.convert(test01);

        System.out.println(test01Converted);

        assertEquals("-0.0025", test01Converted);

    }

}
