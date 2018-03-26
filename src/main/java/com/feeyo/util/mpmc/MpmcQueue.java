package com.feeyo.util.mpmc;

import static com.feeyo.util.mpmc.UnsafeUtil.UNSAFE;



/**
 * 生产者字段的左边，填充 cache line 预防 false sharing
 */ 
class L1Pad {
	protected long p1, p2, p3, p4, p5, p6, p7, p8, p9, p10, p11, p12, p13, p14, p15;
}

/**
 * 生产者
 */ 
class Producer extends L1Pad {
    protected volatile long tail; 

}

/**
 * 在生产者和消费者之间， 填充 cache line 预防 false sharing
 */ 
class L2Pad extends Producer {
	protected long p16, p17, p18, p19, p20, p21, p22, p23, p24, p25, p26, p27, p28, p29, p30;
}
 
/**
 * 消费者
 */ 
class Consumer extends L2Pad {
	protected volatile long head;
}
 
/**
 * 在生产者和消费者之间 fields，填充 cache line 预防 false sharing
 */ 
class L3Pad extends Consumer {
	protected long p31, p32, p33, p34, p35, p36, p37, p38, p39, p40, p41, p42, p43, p44, p45;
}



/**
 * MPMC queue
 * 
 * @see http://www.1024cores.net/home/lock-free-algorithms/queues/bounded-mpmc-queue
 * 
 */
@SuppressWarnings("restriction")
public class MpmcQueue<E> extends L3Pad {
	
	protected static final long TAIL_OFFSET; 
    protected static final long HEAD_OFFSET; 
    protected static final int BUFFER_ARRAY_BASE; 
    protected static final int SHIFT_FOR_SCALE; 
    
    private static final int SEQUENCES_ARRAY_BASE; 
 
	static {
		try {
			
            BUFFER_ARRAY_BASE = UNSAFE.arrayBaseOffset(Object[].class); 
            
            final int scale = UNSAFE.arrayIndexScale(Object[].class);
			if (4 == scale) {
				SHIFT_FOR_SCALE = 2;
			} else if (8 == scale) {
				SHIFT_FOR_SCALE = 3;
			} else {
				throw new IllegalStateException("Unknown pointer size");
			}
            
            TAIL_OFFSET = UNSAFE.objectFieldOffset(Producer.class.getDeclaredField("tail")); 
            HEAD_OFFSET = UNSAFE.objectFieldOffset(Consumer.class.getDeclaredField("head")); 
            
            SEQUENCES_ARRAY_BASE = UNSAFE.arrayBaseOffset(long[].class); 
		} catch (final Exception ex) {
			throw new RuntimeException(ex);
		}
    } 
	
    /**
     * Fast method of finding the next power of 2 greater than or equal to the supplied value. 
     */ 
	public static int findNextPositivePowerOfTwo(final int value) {
		return 1 << (32 - Integer.numberOfLeadingZeros(value - 1));
	}

	private static long calcSequenceOffset(final long sequence, final long mask) {
		return SEQUENCES_ARRAY_BASE + ((sequence & mask) << 3);
	}

	public static long calcSequenceToBufferOffset(final long sequence, final long mask) {
		return BUFFER_ARRAY_BASE + ((sequence & mask) << SHIFT_FOR_SCALE);
	}
	
 
    protected final long mask; 
    protected final int capacity; 
    protected final E[] buffer; 
    
    private final long[] sequences; 
    
    @SuppressWarnings("unchecked") 
	public MpmcQueue(final int requestedCapacity) {
    	
		capacity = findNextPositivePowerOfTwo(requestedCapacity);
		mask = capacity - 1;
		buffer = (E[]) new Object[capacity];

		//
		final long[] sequences = new long[capacity];
		for (int i = 0, size = capacity; i < size; i++) {
			final long sequenceOffset = calcSequenceOffset(i, mask);
			UNSAFE.putOrderedLong(sequences, sequenceOffset, i);
		}

		this.sequences = sequences;
	}
   

	public boolean offer(final E e) {
		if (null == e) {
			throw new NullPointerException("element cannot be null");
		}

		final long mask = this.mask;
		final long[] sequences = this.sequences;

		do {
			final long currentTail = tail;
			final long sequenceOffset = calcSequenceOffset(currentTail, mask);
			final long sequence = UNSAFE.getLongVolatile(sequences, sequenceOffset);

			if (sequence < currentTail) {
				return false;
			}

			if (UNSAFE.compareAndSwapLong(this, TAIL_OFFSET, currentTail, currentTail + 1L)) {
				UNSAFE.putObject(buffer, calcSequenceToBufferOffset(currentTail, mask), e);
				UNSAFE.putOrderedLong(sequences, sequenceOffset, currentTail + 1L);

				return true;
			}
		} while (true);
	} 
 
    @SuppressWarnings("unchecked") 
	public E poll() {
		final long[] sequences = this.sequences;
		final long mask = this.mask;

		do {
			final long currentHead = head;
			final long sequenceOffset = calcSequenceOffset(currentHead, mask);
			final long sequence = UNSAFE.getLongVolatile(sequences, sequenceOffset);
			final long attemptedHead = currentHead + 1L;

			if (sequence < attemptedHead) {
				return null;
			}

			if (UNSAFE.compareAndSwapLong(this, HEAD_OFFSET, currentHead, attemptedHead)) {
				final long elementOffset = calcSequenceToBufferOffset(currentHead, mask);

				final Object e = UNSAFE.getObject(buffer, elementOffset);
				UNSAFE.putObject(buffer, elementOffset, null);
				UNSAFE.putOrderedLong(sequences, sequenceOffset, attemptedHead + mask);

				return (E) e;
			}
		} while (true);
	}
    
 
	public boolean isEmpty() {
		return tail == head;
	}

	public int capacity() {
		return capacity;
	}

	public int remainingCapacity() {
		return capacity() - size();
	}

	public int size() {
		long currentHeadBefore;
		long currentTail;
		long currentHeadAfter = head;

		do {
			currentHeadBefore = currentHeadAfter;
			currentTail = tail;
			currentHeadAfter = head;
		} while (currentHeadAfter != currentHeadBefore);

		return (int) (currentTail - currentHeadAfter);
	}

}
