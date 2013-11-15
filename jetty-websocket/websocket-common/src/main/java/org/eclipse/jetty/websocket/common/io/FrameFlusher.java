//
//  ========================================================================
//  Copyright (c) 1995-2013 Mort Bay Consulting Pty. Ltd.
//  ------------------------------------------------------------------------
//  All rights reserved. This program and the accompanying materials
//  are made available under the terms of the Eclipse Public License v1.0
//  and Apache License v2.0 which accompanies this distribution.
//
//      The Eclipse Public License is available at
//      http://www.eclipse.org/legal/epl-v10.html
//
//      The Apache License v2.0 is available at
//      http://www.opensource.org/licenses/apache2.0.php
//
//  You may elect to redistribute this code under either of these licenses.
//  ========================================================================
//

package org.eclipse.jetty.websocket.common.io;

import java.io.EOFException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicBoolean;

import org.eclipse.jetty.io.AbstractConnection;
import org.eclipse.jetty.io.EndPoint;
import org.eclipse.jetty.util.ArrayQueue;
import org.eclipse.jetty.util.Callback;
import org.eclipse.jetty.util.IteratingCallback;
import org.eclipse.jetty.util.log.Log;
import org.eclipse.jetty.util.log.Logger;
import org.eclipse.jetty.websocket.api.WriteCallback;
import org.eclipse.jetty.websocket.api.extensions.Frame;
import org.eclipse.jetty.websocket.common.Generator;
import org.eclipse.jetty.websocket.common.OpCode;
import org.eclipse.jetty.websocket.common.frames.DataFrame;

/**
 * Interface for working with bytes destined for {@link EndPoint#write(Callback, ByteBuffer...)}
 */
public class FrameFlusher 
{

    private static final Logger LOG = Log.getLogger(FrameFlusher.class);

    /** The endpoint to flush to */
    private final EndPoint endpoint;
    
    /** The websocket generator */
    private final Generator generator;

    private final Object lock = new Object();
    /** Backlog of frames */
    private final ArrayQueue<FrameEntry> queue = new ArrayQueue<>(lock);
    
    private final FlusherCB flusherCB = new FlusherCB();
    
    /** the buffer input size */
    private int bufferSize = 2048;
    /** the gathered write bytebuffer array limit */
    private int gatheredBufferLimit = 10;
    /** Tracking for failure */
    private Throwable failure;
    /** Is WriteBytesProvider closed to more WriteBytes being enqueued? */
    private boolean closed;

    /**
     * Create a WriteBytesProvider with specified Generator and "flush" Callback.
     * 
     * @param generator
     *            the generator to use for converting {@link Frame} objects to network {@link ByteBuffer}s
     * @param endpoint
     *            the endpoint to flush to.
     */
    public FrameFlusher(Generator generator, EndPoint endpoint)
    {
        this.endpoint=endpoint;
        this.generator = Objects.requireNonNull(generator);
    }

    /**
     * Set the buffer size used for generating ByteBuffers from the frames.
     * <p>
     * Value usually obtained from {@link AbstractConnection#getInputBufferSize()}
     * 
     * @param bufferSize
     *            the buffer size to use
     */
    public void setBufferSize(int bufferSize)
    {
        this.bufferSize = bufferSize;
    }
    
    public int getBufferSize()
    {
        return bufferSize;
    }

    /**
     * Force closure of write bytes
     */
    public void close()
    {
        LOG.debug(".close()");
        
        synchronized (lock)
        {
            closed=true;
            
            EOFException eof = new EOFException("Connection has been disconnected");
            flusherCB.failed(eof);
            for (FrameEntry frame : queue)
                frame.notifyFailed(eof);
            queue.clear();
        }
        
    }
    
    /**
     * Used to test for the final frame possible to be enqueued, the CLOSE frame.
     * 
     * @return true if close frame has been enqueued already.
     */
    public boolean isClosed()
    {
        synchronized (lock)
        {
            return closed;
        }
    }

    public void enqueue(Frame frame, WriteCallback callback)
    {
        Objects.requireNonNull(frame);
        FrameEntry entry = new FrameEntry(frame,callback);
        LOG.debug("enqueue({})",entry);
        Throwable failure=null;
        synchronized (lock)
        {
            if (closed)
            {
                // Closed for more frames.
                LOG.debug("Write is closed: {} {}",frame,callback);
                failure=new IOException("Write is closed");
            }
            else if (this.failure!=null)
            {
                failure=this.failure;
            }


            switch (frame.getOpCode())
            {
                case OpCode.PING:
                    queue.add(0,entry);
                    break;
                case OpCode.CLOSE:
                    closed=true;
                    queue.add(entry);
                    break;
                default:
                    queue.add(entry);
            }
        }

        if (failure != null)
        {
            // no changes when failed
            LOG.debug("Write is in failure: {} {}",frame,callback);
            entry.notifyFailed(failure);
            return;
        }
        
        flush();
    }

    void flush()
    {
        flusherCB.iterate();
    }
    
    


    @Override
    public String toString()
    {
        StringBuilder b = new StringBuilder();
        b.append("WriteBytesProvider[");
        if (failure != null)
        {
            b.append(",failure=").append(failure.getClass().getName());
            b.append(":").append(failure.getMessage());
        }
        else
        {
            b.append(",queue.size=").append(queue.size());
        }
        b.append(']');
        return b.toString();
    }

    private class FlusherCB extends IteratingCallback
    {
        private final ArrayQueue<FrameEntry> active = new ArrayQueue<>(lock);
        private final List<ByteBuffer> buffers = new ArrayList<>();
        
        @Override
        protected void completed()
        {       
            // will never be called as process always returns SCHEDULED or IDLE
            throw new IllegalStateException();     
        }

        @Override
        protected State process() throws Exception
        {
            synchronized (lock)
            {
                if (buffers.size()>0)
                    throw new IllegalStateException();
                
                // Do we have any active, not done frames?
                aLoop: for (FrameEntry frame:active)
                {
                    while (!frame.isDone())
                    {
                        buffers.add(frame.getPayloadWindow());
                        if (buffers.size()>=gatheredBufferLimit)
                            break aLoop;
                    }
                }
                
                // can we fit some more active buffers
                qLoop: while (buffers.size()<gatheredBufferLimit && !queue.isEmpty())
                {
                    FrameEntry frame = queue.remove(0);
                    buffers.add(frame.getHeaderBytes());

                    while (!frame.isDone())
                    {
                        buffers.add(frame.getPayloadWindow());
                        if (buffers.size()>=gatheredBufferLimit)
                            break qLoop;
                    }
                }
            }
            
            if (buffers.size()==0)
                return State.IDLE;
            
            // TODO write the buffers somewhere
            endpoint.write(this,buffers.toArray(new ByteBuffer[buffers.size()]));
            buffers.clear();
            return State.SCHEDULED;
        }

        @Override
        public void succeeded()
        {
            for (Iterator<FrameEntry> i=active.iterator();i.hasNext();)
            {
                FrameEntry frame = i.next();
                if (frame.isDone())
                {
                    i.remove();
                    frame.notifySucceeded();
                    frame.freeBuffers();
                }
            }
            super.succeeded();
        }

        @Override
        public void failed(Throwable x)
        {
            for (FrameEntry frame : active)
            {
                frame.notifyFailed(x);
                frame.freeBuffers();
            }
            active.clear();
            super.failed(x);
        }
    }

    private class FrameEntry 
    {
        protected final AtomicBoolean failed = new AtomicBoolean(false);
        protected final Frame frame;
        protected final WriteCallback callback;
        /** holds reference to header ByteBuffer, as it needs to be released on success/failure */
        private ByteBuffer headerBuffer;

        public FrameEntry(Frame frame, WriteCallback callback)
        {
            this.frame = frame;
            this.callback = callback;
        }

        public ByteBuffer getHeaderBytes()
        {
            ByteBuffer buf = generator.generateHeaderBytes(frame);
            headerBuffer = buf;
            return buf;
        }

        public ByteBuffer getPayloadWindow()
        {
            // There is no need to release this ByteBuffer, as it is just a slice of the user provided payload
            return generator.getPayloadWindow(bufferSize,frame);
        }

        public void notifyFailed(Throwable t)
        {
            freeBuffers();
            if (failed.getAndSet(true) == false)
            {
                try
                {
                    if (callback!=null)
                        callback.writeFailed(t);
                }
                catch (Throwable e)
                {
                    LOG.warn("Uncaught exception",e);
                }
            }
        }

        public void notifySucceeded()
        {
            freeBuffers();
            if (callback == null)
            {
                return;
            }
            try
            {
                callback.writeSuccess();
            }
            catch (Throwable t)
            {
                LOG.debug(t);
            }
        }

        public void freeBuffers()
        {
            if (headerBuffer != null)
            {
                generator.getBufferPool().release(headerBuffer);
                headerBuffer = null;
            }

            if (!frame.hasPayload())
            {
                return;
            }

            if (frame instanceof DataFrame)
            {
                DataFrame data = (DataFrame)frame;
                if (data.isPooledBuffer())
                {
                    ByteBuffer payload = frame.getPayload();
                    generator.getBufferPool().release(payload);
                }
            }
        }

        /**
         * Indicate that the frame entry is done generating
         */
        public boolean isDone()
        {
            return frame.remaining() <= 0;
        }
    }
}
