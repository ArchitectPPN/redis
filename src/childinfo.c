/*
 * Copyright (c) 2016, Salvatore Sanfilippo <antirez at gmail dot com>
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are met:
 *
 *   * Redistributions of source code must retain the above copyright notice,
 *     this list of conditions and the following disclaimer.
 *   * Redistributions in binary form must reproduce the above copyright
 *     notice, this list of conditions and the following disclaimer in the
 *     documentation and/or other materials provided with the distribution.
 *   * Neither the name of Redis nor the names of its contributors may be used
 *     to endorse or promote products derived from this software without
 *     specific prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
 * AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
 * IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
 * ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT OWNER OR CONTRIBUTORS BE
 * LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR
 * CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
 * SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
 * INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN
 * CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
 * ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE
 * POSSIBILITY OF SUCH DAMAGE.
 */

#include "server.h"
#include <unistd.h>

/* Open a child-parent channel used in order to move information about the
 * RDB / AOF saving process from the child to the parent (for instance
 * the amount of copy on write memory used) */
void openChildInfoPipe(void) {
    /**
     * pipe是C语言标准库中的一个系统调用，在POSIX兼容系统（如Unix、Linux）中提供。它不是内置函数，而是一个由操作系统提供的功能，允许进程间通信（IPC）。通过调用pipe，用户空间程序可以创建一个单向的数据通道，并获得一对文件描述符来分别进行读取和写入操作。
     * pipe_fds[2] 这个数组的长度必须为2。pipe()函数返回一个包含两个元素的整数数组pipe_fds。这两个元素分别代表管道的读端和写端文件描述符。数组的第一个元素pipe_fds[0]通常用于读取数据，第二个元素pipe_fds[1]用于写入数据。在使用完管道后，通常需要关闭不需要的文件描述符以避免资源泄漏。
     * 在某些情况下，你可能会看到将这两个文件描述符分开存储到不同的变量中，但基本思想是一样的：一个用于读，一个用于写。因此，数组的长度必须至少为2。
     */
    if (pipe(server.child_info_pipe) == -1) {
        /* On error our two file descriptors should be still set to -1,
         * but we call anyway cloesChildInfoPipe() since can't hurt. */
        closeChildInfoPipe();
    } else if (anetNonBlock(NULL,server.child_info_pipe[0]) != ANET_OK) {
        closeChildInfoPipe();
    } else {
        memset(&server.child_info_data,0,sizeof(server.child_info_data));
    }
}

/* Close the pipes opened with openChildInfoPipe(). */
void closeChildInfoPipe(void) {
    if (server.child_info_pipe[0] != -1 ||
        server.child_info_pipe[1] != -1)
    {
        close(server.child_info_pipe[0]);
        close(server.child_info_pipe[1]);
        server.child_info_pipe[0] = -1;
        server.child_info_pipe[1] = -1;
    }
}

/* Send COW data to parent. The child should call this function after populating
 * the corresponding fields it want to sent (according to the process type). */
void sendChildInfo(int ptype) {
    if (server.child_info_pipe[1] == -1) return;
    server.child_info_data.magic = CHILD_INFO_MAGIC;
    server.child_info_data.process_type = ptype;
    ssize_t wlen = sizeof(server.child_info_data);
    if (write(server.child_info_pipe[1],&server.child_info_data,wlen) != wlen) {
        /* Nothing to do on error, this will be detected by the other side. */
    }
}

/* Receive COW data from parent. */
void receiveChildInfo(void) {
    if (server.child_info_pipe[0] == -1) return;
    ssize_t wlen = sizeof(server.child_info_data);
    if (read(server.child_info_pipe[0],&server.child_info_data,wlen) == wlen &&
        server.child_info_data.magic == CHILD_INFO_MAGIC)
    {
        if (server.child_info_data.process_type == CHILD_INFO_TYPE_RDB) {
            server.stat_rdb_cow_bytes = server.child_info_data.cow_size;
        } else if (server.child_info_data.process_type == CHILD_INFO_TYPE_AOF) {
            server.stat_aof_cow_bytes = server.child_info_data.cow_size;
        }
    }
}
