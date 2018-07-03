package com.feeyo.net.nio.util;

public class ProtoUtils {

	private final static int[] sizeTable = { 9, 99, 999, 9999, 99999, 999999, 9999999, 99999999, 999999999, Integer.MAX_VALUE };

	private final static byte[] digitTens = { '0', '0', '0', '0', '0', '0',
			'0', '0', '0', '0', '1', '1', '1', '1', '1', '1', '1', '1', '1',
			'1', '2', '2', '2', '2', '2', '2', '2', '2', '2', '2', '3', '3',
			'3', '3', '3', '3', '3', '3', '3', '3', '4', '4', '4', '4', '4',
			'4', '4', '4', '4', '4', '5', '5', '5', '5', '5', '5', '5', '5',
			'5', '5', '6', '6', '6', '6', '6', '6', '6', '6', '6', '6', '7',
			'7', '7', '7', '7', '7', '7', '7', '7', '7', '8', '8', '8', '8',
			'8', '8', '8', '8', '8', '8', '9', '9', '9', '9', '9', '9', '9',
			'9', '9', '9', };

	private final static byte[] digitOnes = { '0', '1', '2', '3', '4', '5',
			'6', '7', '8', '9', '0', '1', '2', '3', '4', '5', '6', '7', '8',
			'9', '0', '1', '2', '3', '4', '5', '6', '7', '8', '9', '0', '1',
			'2', '3', '4', '5', '6', '7', '8', '9', '0', '1', '2', '3', '4',
			'5', '6', '7', '8', '9', '0', '1', '2', '3', '4', '5', '6', '7',
			'8', '9', '0', '1', '2', '3', '4', '5', '6', '7', '8', '9', '0',
			'1', '2', '3', '4', '5', '6', '7', '8', '9', '0', '1', '2', '3',
			'4', '5', '6', '7', '8', '9', '0', '1', '2', '3', '4', '5', '6',
			'7', '8', '9', };

	private final static byte[] digits = { '0', '1', '2', '3', '4', '5', '6',
			'7', '8', '9', 'a', 'b', 'c', 'd', 'e', 'f', 'g', 'h', 'i', 'j',
			'k', 'l', 'm', 'n', 'o', 'p', 'q', 'r', 's', 't', 'u', 'v', 'w',
			'x', 'y', 'z' };

	public static byte[] convertIntToByteArray(int value) {
		int originValue = value;
		int size = 0;
		if (value < 0) {
			value = -value;
		}

		while (value > sizeTable[size])
			size++;
		size++;
		
		if (originValue < 0)
			size++;

		int q, r;
		byte[] buf = new byte[size];
		if (originValue < 0) {
			buf[0] = '-';
		}
		int charPos = size;

		while (value >= 65536) {
			q = value / 100;
			r = value - ((q << 6) + (q << 5) + (q << 2));
			value = q;
			buf[--charPos] = digitOnes[r];
			buf[--charPos] = digitTens[r];
		}

		for (;;) {
			q = (value * 52429) >>> (16 + 3);
			r = value - ((q << 3) + (q << 1));
			buf[--charPos] = digits[r];
			value = q;
			if (value == 0)
				break;
		}
		return buf;
	}
}
