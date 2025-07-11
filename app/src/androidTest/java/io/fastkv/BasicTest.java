package io.fastkv;

import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;

public class BasicTest {
    @Test
    public void testMergeInvalid() {
        case1();
        case2();
        case3();
        case4();
        case5();
        case6();
    }

    private void case1() {
        ArrayList<Segment> invalids = new ArrayList<>();
        invalids.add(new Segment(0, 1));

        GCHelper.mergeInvalids(invalids);
        Assert.assertEquals(1, invalids.size());
        Assert.assertEquals(1, invalids.get(0).end);

        invalids.add(new Segment(2, 3));

        GCHelper.mergeInvalids(invalids);
        Assert.assertEquals(2, invalids.size());
        Assert.assertEquals(3, invalids.get(1).end);

        invalids.clear();
        invalids.add(new Segment(1, 2));

        GCHelper.mergeInvalids(invalids);
        Assert.assertEquals(1, invalids.size());
        Assert.assertEquals(2, invalids.get(0).end);

    }

    private void case2() {
        ArrayList<Segment> invalids = new ArrayList<>();
        invalids.add(new Segment(0, 1));
        invalids.add(new Segment(1, 2));

        GCHelper.mergeInvalids(invalids);
        Assert.assertEquals(1, invalids.size());
        Assert.assertEquals(2, invalids.get(0).end);

        invalids.add(new Segment(3, 4));
        invalids.add(new Segment(4, 5));
    }

    private void case3() {
        ArrayList<Segment> invalids = new ArrayList<>();
        invalids.add(new Segment(0, 1));
        invalids.add(new Segment(1, 2));

        invalids.add(new Segment(4, 5));
        invalids.add(new Segment(3, 4));

        GCHelper.mergeInvalids(invalids);
        Assert.assertEquals(2, invalids.size());
        Assert.assertEquals(0, invalids.get(0).start);
        Assert.assertEquals(2, invalids.get(0).end);
        Assert.assertEquals(3, invalids.get(1).start);
        Assert.assertEquals(5, invalids.get(1).end);
    }

    private void case4() {
        ArrayList<Segment> invalids = new ArrayList<>();
        invalids.add(new Segment(0, 1));
        invalids.add(new Segment(1, 2));

        invalids.add(new Segment(3, 4));
        invalids.add(new Segment(4, 5));
        invalids.add(new Segment(5, 6));

        GCHelper.mergeInvalids(invalids);
        Assert.assertEquals(2, invalids.size());
        Assert.assertEquals(0, invalids.get(0).start);
        Assert.assertEquals(2, invalids.get(0).end);
        Assert.assertEquals(3, invalids.get(1).start);
        Assert.assertEquals(6, invalids.get(1).end);
    }

    private void case5() {
        ArrayList<Segment> invalids = new ArrayList<>();
        invalids.add(new Segment(0, 1));
        invalids.add(new Segment(1, 2));

        invalids.add(new Segment(3, 4));
        invalids.add(new Segment(4, 5));
        invalids.add(new Segment(5, 6));

        invalids.add(new Segment(7, 8));

        GCHelper.mergeInvalids(invalids);
        Assert.assertEquals(3, invalids.size());
        Assert.assertEquals(2, invalids.get(0).end);
        Assert.assertEquals(6, invalids.get(1).end);
        Assert.assertEquals(8, invalids.get(2).end);
    }

    private void case6() {
        ArrayList<Segment> invalids = new ArrayList<>();
        invalids.add(new Segment(3, 4));
        invalids.add(new Segment(1, 3));
        invalids.add(new Segment(0, 1));
        invalids.add(new Segment(4, 5));

        GCHelper.mergeInvalids(invalids);
        Assert.assertEquals(1, invalids.size());

        invalids.clear();
        invalids.add(new Segment(0, 1));
        invalids.add(new Segment(2, 3));
        invalids.add(new Segment(5, 6));
        invalids.add(new Segment(7, 8));
        Assert.assertEquals(4, invalids.size());
    }
}
