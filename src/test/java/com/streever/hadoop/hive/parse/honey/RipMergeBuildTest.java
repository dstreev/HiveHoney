package com.streever.hadoop.hive.parse.honey;

import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.*;

/**
 * Created by dstreev on 2016-08-01.
 */
public class RipMergeBuildTest {


    @Test
    public void innerEval() throws Exception {
        String test1 = ";2;11;2;5;20160515;2;1.000000000;5;20160415;2;1.000000000;5;20160315;2;1.000000000;5;20160215;2;1.000000000;5;20160115;2;1.000000000;5;20151215;2;1.000000000;5;20151115;2;1.000000000;5;20151015;2;1.000000000;5;20150915;2;1.000000000;5;20150815;2;1.000000000;5;20150701;2;1.0650000000;";

        List<Integer> groupItems = new ArrayList<Integer>();
        groupItems.add(1);
        groupItems.add(3);

        String check = RipMergeBuild.innerEval(4,4,groupItems,test1,";",",");

        System.out.println("Source Test Record: " + test1);
        System.out.println("Offset      : 4");
        System.out.println("Group Size  : 4");
        System.out.println("Item Indexes: 1,3");
        System.out.println("Delimiter   : ';'");
        System.out.println("Build Sep   : ','");
        System.out.println("Result: " + check);

    }

    @Test
    public void innerEval2() throws Exception {
        String test1 = ";23";

        List<Integer> groupItems = new ArrayList<Integer>();
        groupItems.add(1);
//        groupItems.add(3);

        String check = RipMergeBuild.innerEval(0,2,groupItems,test1,";",",");

        System.out.println("Source Test Record: " + test1);
        System.out.println("Offset      : 4");
        System.out.println("Group Size  : 4");
        System.out.println("Item Indexes: 1,3");
        System.out.println("Delimiter   : ';'");
        System.out.println("Build Sep   : ','");
        System.out.println("Result: " + check);

    }

    @Test
    public void innerEval3() throws Exception {
        String test1 = ";2;0;2;";

        List<Integer> groupItems = new ArrayList<Integer>();
        groupItems.add(1);
//        groupItems.add(3);

        String check = RipMergeBuild.innerEval(2,2,groupItems,test1,";",",");

        System.out.println("Source Test Record: " + test1);
        System.out.println("Offset      : 2");
        System.out.println("Group Size  : 2");
        System.out.println("Item Indexes: 1");
        System.out.println("Delimiter   : ';'");
        System.out.println("Build Sep   : ','");
        System.out.println("Result: " + check);

    }

    @Test
    public void innerEval4() throws Exception {
        String test1 = "";

        List<Integer> groupItems = new ArrayList<Integer>();
        groupItems.add(1);
//        groupItems.add(3);

        String check = RipMergeBuild.innerEval(0,2,groupItems,test1,";",",");

        System.out.println("Source Test Record: " + test1);
        System.out.println("Offset      : 2");
        System.out.println("Group Size  : 2");
        System.out.println("Item Indexes: 1");
        System.out.println("Delimiter   : ';'");
        System.out.println("Build Sep   : ','");
        System.out.println("Result: " + check);

    }

}