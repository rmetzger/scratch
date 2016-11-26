package com.dataartisans;

import java.util.TreeMap;

/**
 * Created by robert on 11/24/16.
 */
public class TreeTest {

    public static class Customer {
        public String name;
        public Customer(String name) {
            this.name = name;
        }

        @Override
        public String toString() {
            return "Customer{" +
                    "name='" + name + '\'' +
                    '}';
        }
    }

    public static void main(String[] args) {
        TreeMap<Long, Customer> time = new TreeMap<>();

        time.put(10L, new Customer("name at time 10"));
        time.put(30L, new Customer("name at time 30"));
        time.put(60L, new Customer("name at time 60"));
        time.put(90L, new Customer("name at time 90"));

        // get valid customer at time 35 (which is 30)
        Long c = time.floorKey(35L);
        System.out.println("c = " + c);
    }
}
