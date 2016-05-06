/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.elasticsearch.bootstrap;

import com.sun.jna.LastErrorException;
import com.sun.jna.Library;
import com.sun.jna.Native;
import com.sun.jna.Structure;
import org.apache.lucene.util.Constants;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.Loggers;

import java.io.IOException;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.List;

/**
 */
public class Quotactl {

    private static final ESLogger logger = Loggers.getLogger(Quotactl.class);
    // null if unavailable or something goes wrong.
    private static final LinuxLibrary linux_libc;
    public static final int USRQUOTA = 0;
    public static final int GRPQUOTA = 1;
    public static final int Q_GETQUOTA = 0x800007;

    private static final int SUBCMDMASK = 0x00ff;
    private static final int SUBCMDSHIFT = 8;

    private static final int QCMD(int cmd, int type) {
        return (((cmd) << SUBCMDSHIFT) | ((type) & SUBCMDMASK));
    }


    /* Flags in dqb_valid that indicate which fields in
                      dqblk structure are valid. */
    public static final int QIF_BLIMITS = 1;
    public static final int QIF_SPACE = 2;
    public static final int QIF_ILIMITS = 4;
    public static final int QIF_INODES = 8;
    public static final int QIF_BTIME = 16;
    public static final int QIF_ITIME = 32;
    public static final int QIF_LIMITS = (QIF_BLIMITS | QIF_ILIMITS);
    public static final int QIF_USAGE = (QIF_SPACE | QIF_INODES);
    public static final int QIF_TIMES = (QIF_BTIME | QIF_ITIME);
    public static final int QIF_ALL = (QIF_LIMITS | QIF_USAGE | QIF_TIMES);

    static {
        LinuxLibrary lib = null;
        if (Constants.LINUX) {
            try {
                lib = (LinuxLibrary) Native.loadLibrary("c", LinuxLibrary.class);
            } catch (UnsatisfiedLinkError e) {
                logger.warn("unable to link C library. native methods (seccomp) will be disabled.", e);
            }
        }
        linux_libc = lib;
    }

    public static boolean isLoaded() {
        return linux_libc != null;
    }


    /** Access to non-standard Linux libc methods */
    interface LinuxLibrary extends Library {
        int quotactl(int cmd, String special, int id, Structure addr) throws LastErrorException;
    };

    /**
     * struct dqblk { // Definition since Linux 2.4.22
     * u_int64_t dqb_bhardlimit;	// absolute limit on disk bytes alloc
     * u_int64_t dqb_bsoftlimit;	// preferred limit on disk bytes
     * u_int64_t dqb_curspace;	    // current quota block count
     * u_int32_t dqb_ihardlimit;	// maximum # allocated inodes + 1
     * u_int32_t dqb_isoftlimit;	// preferred inode limit
     * u_int32_t dqb_curinodes;	    // current # allocated inodes
     * u_int32_t dqb_btime;		    // time limit for excessive disk use
     * u_int32_t dqb_itime;		    // time limit for excessive files
     * u_int32_t dqb_id;		    // bit mask of QIF_* constants
     * };
     *
     */
     public static final class dqblk extends Structure implements Structure.ByReference {
        public long dqb_bhardlimit;
        public long dqb_bsoftlimit;
        public long dqb_curspace;
        public long dqb_ihardlimit;
        public long dqb_isoftlimit;
        public long dqb_curinodes;
        public long dqb_btime;
        public long dqb_itime;
        public int dqb_id;

        @Override
        protected List<String> getFieldOrder() {
            return Arrays.asList("dqb_bhardlimit",
                "dqb_bsoftlimit", "dqb_curspace", "dqb_ihardlimit",
                "dqb_isoftlimit", "dqb_curinodes", "dqb_btime",
                "dqb_itime", "dqb_id"
            );
        }
    };

    public static dqblk get_quota(String blockDevice, int userOrGroupId, int type) throws IOException {
        if (Constants.LINUX) {
            if (linux_libc == null) {
                throw new UnsupportedOperationException("quotactl unavailable");
            }
            dqblk struct = new dqblk();
            try {
                linux_libc.quotactl(QCMD(Q_GETQUOTA, type), blockDevice, userOrGroupId, struct);
            } catch (LastErrorException ex) {
                throw new IOException("failed to fetch quota", ex);
            }
        }
        return null;
    }

    public static long getAvaliableSpace(String blockDevice, int user, long blockSize) {
        try {
            dqblk quota = get_quota(
                blockDevice
                , user, USRQUOTA);
            return quota == null ? -1 : quota.dqb_bhardlimit * blockSize;
        } catch (IOException e) {
            logger.warn("Failed to get quota", e);
            return -1;
        }
    }

    public static long getUsedSpace(String blockDevice, int user, long blockSize) {
        try {
            dqblk quota = get_quota(
                blockDevice
                , user, USRQUOTA);
            return quota == null ? -1 : quota.dqb_curspace * blockSize;
        } catch (IOException e) {
            logger.warn("Failed to get quota", e);
            return -1;
        }
    }
}
