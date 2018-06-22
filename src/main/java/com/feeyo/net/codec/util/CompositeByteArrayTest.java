package com.feeyo.net.codec.util;

import java.util.Arrays;

public class CompositeByteArrayTest {
    public static void main(String[] args) {
        CompositeByteArray a = new CompositeByteArray();
        a.add("0123".getBytes());
        a.add("9994".getBytes());
        a.add("4567".getBytes());
        a.add("89".getBytes());
        a.add("abcdefghijklmn".getBytes());
        int index2 = a.firstIndex(0, (byte) '2');
        System.out.println("The value in index " + index2 + " is: " + (char) a.get(index2));

        int index4 = a.firstIndex(4, (byte) '4');
        System.out.println("The value in index " + index4 + " is: " + (char) a.get(index4));

        int index7 = a.firstIndex(4, (byte) '7');
        System.out.println("The value in index " + index7 + " is: " + (char) a.get(index7));

        int indexn = a.firstIndex(4, (byte) 'n');
        System.out.println("The value in index " + indexn + " is: " + (char) a.get(indexn));

        int index9 = a.firstIndex(5, (byte) '9');
        System.out.println("The value in index " + index9 + " is: " + (char) a.get(index9));

        System.out.println("The sub array is: " + Arrays.toString(a.subArray(2, 10)));


        CompositeByteArray b = new CompositeByteArray();
        b.add("0123456789abcdefghijklmn".getBytes());
        int index6 = b.firstIndex(3, (byte) '6');
        System.out.println("The value in index " + index6 + " is: " + (char) b.get(index6));

        int index8 = b.firstIndex(7, (byte) '8');
        System.out.println("The value in index " + index8 + " is: " + (char) b.get(index8));

        System.out.println("The sub array is: " + Arrays.toString(b.subArray(2, 10)));
    }
}
