package com.cibtc.yuwt.datamigration;

import java.lang.instrument.Instrumentation;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by yuwt on 2015/12/22.
 */
public final class ObjectSizeFetcher {

    private static Instrumentation instrumentation;

//    private ObjectSizeFetcher() {};

    public static void premain(String args, Instrumentation isnt) {
        System.out.println("premain...");
        instrumentation = isnt;
    }

    public static void agentmain(String args, Instrumentation isnt) {
        System.out.println("agentmain...");
        instrumentation = isnt;
    }

    /**
     * Return the approximate size in bytes.
     *
     * @param obj
     * @return
     */
    public static long getObjectSize(Object obj) {
        if (instrumentation == null) {
            throw new IllegalStateException("Agent not initialized.");
        }
        return instrumentation.getObjectSize(obj);
    }

    public static void main(String... args) {
        List<String> list = new ArrayList<String>();
        for (int i = 0; i < 100; i++) {
            list.add("string: " + i);
        }
        System.out.print("object size: " + ObjectSizeFetcher.getObjectSize(list));
    }
}
