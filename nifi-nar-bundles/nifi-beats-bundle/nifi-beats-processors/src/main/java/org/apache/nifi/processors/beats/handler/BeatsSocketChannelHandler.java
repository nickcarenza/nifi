/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.nifi.processors.beats.handler;

import java.io.IOException;
import java.net.InetAddress;
import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.nio.channels.SocketChannel;
import java.nio.charset.Charset;
import java.util.List;
import java.util.concurrent.BlockingQueue;

import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.processor.util.listen.dispatcher.AsyncChannelDispatcher;
import org.apache.nifi.processor.util.listen.event.Event;
import org.apache.nifi.processor.util.listen.event.EventFactory;
import org.apache.nifi.processor.util.listen.handler.socket.StandardSocketChannelHandler;
import org.apache.nifi.processor.util.listen.response.socket.SocketChannelResponder;
import org.apache.nifi.processors.beats.frame.BeatsDecoder;
import org.apache.nifi.processors.beats.frame.BeatsFrame;
import org.apache.nifi.processors.beats.frame.BeatsFrameException;

/**
 * Extends the StandardSocketChannelHandler to decode bytes into Beats frames.
 */
public class BeatsSocketChannelHandler<E extends Event<SocketChannel>> extends StandardSocketChannelHandler<E> {

    private BeatsDecoder decoder;
    private BeatsFrameHandler<E> frameHandler;

    public BeatsSocketChannelHandler(final SelectionKey key,
                                     final AsyncChannelDispatcher dispatcher,
                                     final Charset charset,
                                     final EventFactory<E> eventFactory,
                                     final BlockingQueue<E> events,
                                     final ComponentLog logger) {
        super(key, dispatcher, charset, eventFactory, events, logger);
        this.decoder = new BeatsDecoder(charset);
        this.frameHandler = new BeatsFrameHandler<>(key, charset, eventFactory, events, dispatcher, logger);
    }

    @Override
    protected void processBuffer(final SocketChannel socketChannel, final ByteBuffer socketBuffer)
            throws InterruptedException, IOException {

        // get total bytes in buffer
        final int total = socketBuffer.remaining();
        final InetAddress sender = socketChannel.socket().getInetAddress();

        try {
            // go through the buffer parsing the packet command
            for (int i = 0; i < total; i++) {
                byte currByte = socketBuffer.get();

                // if we found the end of a frame, handle the frame and mark the buffer
                if (decoder.process(currByte)) {

                    final List<BeatsFrame> frames = decoder.getFrames();

                    for (BeatsFrame frame : frames) {
                        logger.debug("Received Beats frame with transaction {} and command {}",
                                new Object[]{frame.getSeqNumber(), frame.getSeqNumber()});
                        // Ignore the WINDOW SIZE type frames as they contain no payload.
                        if (frame.getFrameType() != 0x57) {
                            final SocketChannelResponder responder = new SocketChannelResponder(socketChannel);
                            frameHandler.handle(frame, responder, sender.toString());
                        }
                    }
                    socketBuffer.mark();
                }
            }
            logger.debug("Done processing buffer");

        } catch (final BeatsFrameException rfe) {
            logger.error("Error reading Beats frames due to {}", new Object[] {rfe.getMessage()}, rfe);
            // if an invalid frame or bad data was sent then the decoder will be left in a
            // corrupted state, so lets close the connection and cause the client to re-establish
            dispatcher.completeConnection(key);
        }
    }

    // not used for anything in Beats since the decoder encapsulates the delimiter
    @Override
    public byte getDelimiter() {
        return BeatsFrame.DELIMITER;
    }

}
