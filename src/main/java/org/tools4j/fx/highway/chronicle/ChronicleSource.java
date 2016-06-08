/**
 * The MIT License (MIT)
 *
 * Copyright (c) 2016 fx-highway (tools4j), Marco Terzer
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in all
 * copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 * SOFTWARE.
 */
package org.tools4j.fx.highway.chronicle;

import net.openhft.chronicle.Chronicle;
import net.openhft.chronicle.ChronicleQueueBuilder;
import net.openhft.chronicle.ExcerptAppender;
import net.openhft.chronicle.tcp.TcpConnectionHandler;
import net.openhft.chronicle.tcp.TcpConnectionListener;
import org.tools4j.fx.highway.util.FileUtil;

import java.io.File;
import java.io.IOException;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;

public class ChronicleSource {

    private TcpConnectionListener connectionListener = new TcpConnectionHandler() {
        @Override
        public void onConnect(SocketChannel channel) {
            System.out.println("CONNECT: address=" + channel.socket().getLocalSocketAddress() + ", port=" + channel.socket().getLocalPort());
        }

        @Override
        public void onListen(ServerSocketChannel channel) {
            System.out.println("LISTEN, address=" + channel.socket().getLocalSocketAddress() + ", port=" + channel.socket().getLocalPort());
        }
    };
    private Chronicle queue;

    public ChronicleSource() throws IOException {
        this("localhost", 1234);
    }
    public ChronicleSource(final String host, final int port) throws IOException {
        FileUtil.deleteTmpDirFilesMatching("chronicle-source");
        final File basePath = FileUtil.tmpDirFile("chronicle-source");

        this.queue = ChronicleQueueBuilder
                .indexed(basePath.getPath())
                .source()
                .acceptorMaxThreads(3)
                .bindAddress(host, port)
                .connectionListener(connectionListener)
                .build();
        this.queue.clear();
    }

    public ExcerptAppender createAppender() {
        try {
            return queue.createAppender();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public void close() throws IOException {
        queue.close();
        queue = null;
    }
}
