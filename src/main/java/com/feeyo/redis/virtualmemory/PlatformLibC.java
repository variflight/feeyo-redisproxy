package com.feeyo.redis.virtualmemory;

import com.sun.jna.Library;
import com.sun.jna.Native;
import com.sun.jna.NativeLong;
import com.sun.jna.Platform;
import com.sun.jna.Pointer;

public class PlatformLibC {
    private static final boolean isWindows = Platform.isWindows();
    private static CLibrary cLibrary = CLibrary.INSTANCE;
    
    public static final int MADV_WILLNEED = 3;
    
    public static int mlock(Pointer address, NativeLong size) {
        if (isWindows) {
            return cLibrary.VirtualLock(address, size);
        } else {
            return cLibrary.mlock(address, size);
        }
    }
    
    public static int munlock(Pointer address, NativeLong size) {
        if (isWindows) {
            return cLibrary.VirtualUnlock(address, size);
        } else {
            return cLibrary.munlock(address, size);
        }
    }
    
    public static int madvise(Pointer address, NativeLong size, int advice) {
        if (isWindows) {
            return 0; // no implementation in Windows 7 and earlier
        } else {
            return cLibrary.madvise(address, size, advice);
        }
    }
    
    interface CLibrary extends Library {
        CLibrary INSTANCE = (CLibrary) Native.loadLibrary(Platform.isWindows() ? "kernel32" : "c", CLibrary.class);

        /**
         * call for windows
         * @param address A pointer to the base address of the region of pages to be unlocked
         * @param size The size of the region being unlocked, in bytes. 
         * <strong>NOTE:</strong> a 2-byte range straddling a page boundary causes both pages to be unlocked.
         * @return If the function succeeds, the return value is nonzero otherwise the return value is zero.
         */
        int VirtualLock(Pointer address, NativeLong size);
        
        /**
         * call for windows
         * @param address A pointer to the base address of the region of pages to be unlocked.
         * @param size The size of the region being unlocked, in bytes.
         * <strong>NOTE:</strong> a 2-byte range straddling a page boundary causes both pages to be unlocked.
         * @return If the function succeeds, the return value is nonzero otherwise the return value is zero.
         */
        int VirtualUnlock(Pointer address, NativeLong size);
        
        // call for linux
        int mlock(Pointer address, NativeLong size);
        int munlock(Pointer address, NativeLong size);
        int madvise(Pointer address, NativeLong size, int advice);
    }
}
