package com.ybt.seaotter;

import com.google.common.collect.Lists;

import java.util.List;

public class ParallelTest {

    public static void main(String[] args) {
        List<Integer> list = Lists.newArrayList(1, 2, 3, 4, 5, 6, 7, 8, 9, 10);
        list.parallelStream().forEach(e -> {
            try {
                Thread.sleep(1000);
            } catch (InterruptedException ex) {
                throw new RuntimeException(ex);
            }
            System.out.println(e);
        });
        System.out.println("Done");
    }
}
