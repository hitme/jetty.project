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

package org.eclipse.jetty.websocket.common.io.payload;

import java.nio.ByteBuffer;

import org.eclipse.jetty.util.BufferUtil;
import org.eclipse.jetty.websocket.api.extensions.Frame;

public class DeMaskProcessor implements PayloadProcessor
{
    private byte maskBytes[];
    private int maskOffset;

    @Override
    public void process(ByteBuffer payload, ByteBuffer destination)
    {
        if (maskBytes == null)
        {
            if (payload!=destination)
                BufferUtil.put(payload,destination);
            return;
        }

        int maskInt = ByteBuffer.wrap(maskBytes).getInt();
        int from = payload.position();
        int to = destination.position();
        int end = payload.limit();
        int offset = this.maskOffset;
        int remaining;
        while ((remaining = end - from) > 0)
        {
            if (remaining >= 4 && (offset % 4) == 0)
            {
                destination.putInt(to,payload.getInt(from) ^ maskInt);
                from += 4;
                to += 4;
                offset += 4;
            }
            else
            {
                destination.put(to,(byte)(payload.get(from) ^ maskBytes[offset & 3]));
                ++from;
                ++to;
                ++offset;
            }
        }
        
        if (payload!=destination)
            destination.position(destination.position()+payload.remaining());
        
        maskOffset = offset;
    }

    public void reset(byte mask[])
    {
        this.maskBytes = mask;
        this.maskOffset = 0;
    }

    @Override
    public void reset(Frame frame)
    {
        reset(frame.getMask());
    }
}
