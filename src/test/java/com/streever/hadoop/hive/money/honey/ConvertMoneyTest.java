package com.streever.hadoop.hive.money.honey;

import org.junit.Test;
import static org.junit.Assert.*;

/**
 * Created by dstreev on 2016-10-30.
 */
public class ConvertMoneyTest {

    @Test
    public void ConvertMoney001() {
        String test01 = "($234M)";
        String test01Converted = ConvertMoney.convert(test01);

        System.out.println(test01Converted);

        assertEquals("-234000000", test01Converted);

    }

    @Test
    public void ConvertMoney002() {
        String test01 = "($853,071)";
        String test01Converted = ConvertMoney.convert(test01);

        System.out.println(test01Converted);

        assertEquals("-853071", test01Converted);

    }

    @Test
    public void ConvertMoney003() {
        String test01 = "$1,763M";
        String test01Converted = ConvertMoney.convert(test01);

        System.out.println(test01Converted);

        assertEquals("1763000000", test01Converted);

    }

    @Test
    public void ConvertMoney004() {
        String test01 = "$4,207,186";
        String test01Converted = ConvertMoney.convert(test01);

        System.out.println(test01Converted);

        assertEquals("4207186", test01Converted);

    }

    @Test
    public void ConvertMoney005() {
        String test01 = "($4'207'186)";
        String test01Converted = ConvertMoney.convert(test01);

        System.out.println(test01Converted);

        assertEquals("-4207186", test01Converted);

    }

}
