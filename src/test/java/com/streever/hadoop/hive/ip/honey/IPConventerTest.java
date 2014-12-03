package com.streever.hadoop.hive.ip.honey;

import org.junit.Test;
import static org.junit.Assert.*;
/**
 * Created by dstreev on 12/3/14.
 */
public class IPConventerTest {

    @Test
    public void Test01() {
        assertTrue(IPConverter.convertFromString("10.0.23.43") == 167778091l);
        assertTrue(IPConverter.convertFromLong(167778091l).equals("10.0.23.43"));
    }
}
