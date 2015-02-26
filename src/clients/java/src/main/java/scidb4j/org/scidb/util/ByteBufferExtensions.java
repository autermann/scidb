package org.scidb.util;

import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;

public class ByteBufferExtensions
{
    public static BigInteger getUnsignedLong(ByteBuffer buf)
    {
        byte[] b = new byte[8];
        buf.get(b);
        
        if (buf.order() == ByteOrder.BIG_ENDIAN)
        {
            return new BigInteger(1, b);
        }
        else
        {
            byte[] swapped = new byte[b.length];
            for (int i = 0; i < swapped.length; i++)
            {
                swapped[i] = b[b.length - 1 - i];
            }
            return new BigInteger(1, swapped);
        }
    }

    public static long getUnsignedInt(ByteBuffer buf)
    {
        byte[] b = new byte[4];
        buf.get(b);

        if (buf.order() == ByteOrder.BIG_ENDIAN)
        {
            return new BigInteger(1, b).longValue();
        }
        else
        {
            byte[] swapped = new byte[b.length];
            for (int i = 0; i < swapped.length; i++)
            {
                swapped[i] = b[b.length - 1 - i];
            }
            return new BigInteger(1, swapped).longValue();
        }
    }

    public static int getUnsignedShort(ByteBuffer buf)
    {
        byte[] b = new byte[2];
        buf.get(b);

        if (buf.order() == ByteOrder.BIG_ENDIAN)
        {
            return new BigInteger(1, b).intValue();
        }
        else
        {
            byte[] swapped = new byte[b.length];
            for (int i = 0; i < swapped.length; i++)
            {
                swapped[i] = b[b.length - 1 - i];
            }
            return new BigInteger(1, swapped).intValue();
        }
    }

    public static short getUnsignedByte(ByteBuffer buf)
    {
        return buf.get();
    }
}
