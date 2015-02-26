package org.scidb.client;

import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.logging.Logger;

import org.scidb.util.ByteBufferExtensions;

public class Chunk implements IChunk
{
    private int attributeId;
    private long arrayId;
    private boolean eof;
    private boolean rle;
    private boolean sparse;

    // Fields for RLE chunk header
    private ByteBuffer chunkData;
    private Header header;
    private Segment[] segments;
    private int payloadStart;

    // Fields for iterating over items
    private int curSeg = 0;
    private int curItemInSeg = 0;
    private int curValueIndex = 0;
    private boolean _end = false;

    long[] startPos;
    long[] endPos;
    long[] chunkLen;
    private int elementNumber = 0;

    private static Logger log = Logger.getLogger(Chunk.class.getName());

    public Chunk(org.scidb.io.network.SciDBNetworkMessage.Chunk msg, Array array) throws SciDBException
    {
        org.scidb.io.network.ScidbMsg.Chunk record = msg.getRecord();
        attributeId = record.getAttributeId();
        arrayId = record.getArrayId();
        eof = record.getEof();
        rle = record.getRle();
        sparse = record.getSparse();

        if (!eof)
        {
            int coordCount = record.getCoordinatesCount();
            startPos = new long[coordCount];
            endPos = new long[coordCount];
            chunkLen = new long[coordCount];
            for (int dimNo = 0; dimNo < record.getCoordinatesCount(); dimNo++)
            {
                long chunkCoordinate = record.getCoordinates(dimNo);
                Schema.Dimension dim = array.getSchema().getDimensions()[dimNo];
                long endCoord = chunkCoordinate + dim.getChunkInterval() - 1;
                if (endCoord > dim.getEndMax())
                    endCoord = dim.getEndMax();
                startPos[dimNo] = chunkCoordinate;
                endPos[dimNo] = endCoord;
                chunkLen[dimNo] = endCoord - chunkCoordinate + 1;
            }

            chunkData = ByteBuffer.wrap(msg.getData());
            chunkData.order(ByteOrder.LITTLE_ENDIAN);
            int compressionMethod = record.getCompressionMethod();

            if (array.getSchema().getAttributes()[attributeId].isEmptyIndicator())
            {
                log.fine("Got bitmap chunk");
            } else
            {
                log.fine("Got just chunk");
            }

            if (compressionMethod != 0)
            {
                throw new SciDBException("Compressed chunks not yet supported");
            }

            if (!rle)
            {
                throw new SciDBException("Non RLE chunks not yet supported");
            }

            if (sparse)
            {
                throw new SciDBException("Sparse chunks not yet supported");
            }

            header = new Header(chunkData);

            segments = new Segment[header.nSegs + 1];
            for (int i = 0; i < segments.length; i++)
            {
                segments[i] = new Segment(chunkData);
            }
            if (segments.length == 0)
                _end = true;

            payloadStart = chunkData.position();

            evalCurValueIndex();
        }
    }

    public void evalCurValueIndex()
    {
        if (_end)
            return;
        Segment s = segments[curSeg];
        if (!s.isNull)
        {
            int size = header.elemSize == 0 ? 4 : (int) header.elemSize;
            if (s.same)
            {
                curValueIndex = payloadStart + s.valueIndex * size;
            } else
            {
                curValueIndex = payloadStart + (s.valueIndex + curItemInSeg) * size;
            }
        }
    }



    public int getAttributeId()
    {
        return attributeId;
    }

    public boolean endOfArray()
    {
        return eof;
    }

    public boolean isSparse()
    {
        return sparse;
    }

    public boolean isRle()
    {
        return rle;
    }

    public boolean endOfChunk()
    {
        return _end;
    }

    public boolean hasNext()
    {
        int _curSeg = curSeg;
        int _curItemInSeg = curItemInSeg;
        int _curValueIndex = curValueIndex;
        int _curElementNumber = elementNumber;
        boolean r = move();
        curSeg = _curSeg;
        curItemInSeg = _curItemInSeg;
        curValueIndex = _curValueIndex;
        elementNumber = _curElementNumber;
        return r;
    }

    public boolean move()
    {
        if (_end || (curSeg == segments.length - 1))
        {
            return false;
        }
        Segment s = segments[curSeg];
        long len = segments[curSeg + 1].pPosition - s.pPosition;
        while (true)
        {
            curItemInSeg++;
            if (curItemInSeg < len)
            {
                elementNumber++;
                evalCurValueIndex();
                return true;
            } else
            {
                curSeg++;
                curItemInSeg = -1; // It will be reset to 0 on the next increment
                if (curSeg == segments.length - 1)
                {
                    _end = true;
                    return false;
                }
                s = segments[curSeg];
                len = segments[curSeg + 1].pPosition - s.pPosition;
            }
        }
    }

    public boolean isNull()
    {
        return segments[curSeg].isNull;
    }

    public long getInt64() throws SciDBException
    {
        if (!segments[curSeg].isNull || _end)
        {
            chunkData.position(curValueIndex);
            return chunkData.getLong();
        } else
        {
            throw new SciDBException("Current item is NULL");
        }
    }

    public int getInt32() throws SciDBException
    {
        if (!segments[curSeg].isNull || _end)
        {
            chunkData.position(curValueIndex);
            return chunkData.getInt();
        } else
        {
            throw new SciDBException("Current item is NULL");
        }
    }

    public short getInt16() throws SciDBException
    {
        if (!segments[curSeg].isNull || _end)
        {
            chunkData.position(curValueIndex);
            return chunkData.getShort();
        } else
        {
            throw new SciDBException("Current item is NULL");
        }
    }

    public byte getInt8() throws SciDBException
    {
        if (!segments[curSeg].isNull || _end)
        {
            chunkData.position(curValueIndex);
            return chunkData.get();
        } else
        {
            throw new SciDBException("Current item is NULL");
        }
    }

    public BigInteger getUint64() throws SciDBException
    {
        if (!segments[curSeg].isNull || _end)
        {
            chunkData.position(curValueIndex);
            return ByteBufferExtensions.getUnsignedLong(chunkData);
        } else
        {
            throw new SciDBException("Current item is NULL");
        }
    }

    public long getUint32() throws SciDBException
    {
        if (!segments[curSeg].isNull || _end)
        {
            chunkData.position(curValueIndex);
            return ByteBufferExtensions.getUnsignedInt(chunkData);
        } else
        {
            throw new SciDBException("Current item is NULL");
        }
    }

    public int getUint16() throws SciDBException
    {
        if (!segments[curSeg].isNull || _end)
        {
            chunkData.position(curValueIndex);
            return ByteBufferExtensions.getUnsignedShort(chunkData);
        } else
        {
            throw new SciDBException("Current item is NULL");
        }
    }

    public short getUint8() throws SciDBException
    {
        if (!segments[curSeg].isNull || _end)
        {
            chunkData.position(curValueIndex);
            return ByteBufferExtensions.getUnsignedByte(chunkData);
        } else
        {
            throw new SciDBException("Current item is NULL");
        }
    }

    public char getChar() throws SciDBException
    {
        if (!segments[curSeg].isNull || _end)
        {
            chunkData.position(curValueIndex);
            return (char) chunkData.get();
        } else
        {
            throw new SciDBException("Current item is NULL");
        }
    }

    public float getFloat() throws SciDBException
    {
        if (!segments[curSeg].isNull || _end)
        {
            chunkData.position(curValueIndex);
            return chunkData.getFloat();
        } else
        {
            throw new SciDBException("Current item is NULL");
        }
    }

    public double getDouble() throws SciDBException
    {
        if (!segments[curSeg].isNull || _end)
        {
            chunkData.position(curValueIndex);
            return chunkData.getDouble();
        } else
        {
            throw new SciDBException("Current item is NULL");
        }
    }

    public boolean getBoolean() throws SciDBException
    {
        if (!segments[curSeg].isNull || _end)
        {
            int p = curValueIndex - payloadStart;
            chunkData.position(payloadStart + (p >> 3) );
            byte b = chunkData.get();
            return (b & (1 << (p & 7))) != 0;
        } else
        {
            throw new SciDBException("Current item is NULL");
        }
    }

    public String getString() throws SciDBException
    {
        if (!segments[curSeg].isNull || _end)
        {
            chunkData.position(curValueIndex);
            int offset = chunkData.getInt();
            chunkData.position(payloadStart + (int) header.varOffs + offset);
            byte b = chunkData.get();
            int len;
            if (b == 0)
                len = chunkData.getInt();
            else
                len = b;
            byte[] chars = new byte[len - 1];
            chunkData.get(chars);
            return new String(chars);
        } else
        {
            throw new SciDBException("Current item is NULL");
        }
    }

    public long[] getCoordinates()
    {
        long l = elementNumber;
        long[] currPos = new long[chunkLen.length];
        for (int i = chunkLen.length - 1; i >= 0; i--)
        {
           currPos[i] = startPos[i] + l % chunkLen[i];
           l /= chunkLen[i];
        }
        return currPos;
    }

    public long getArrayId()
    {
        return arrayId;
    }

    public static class Header
    {
        final BigInteger RlePayloadMagic = new BigInteger("15987121899357252268");
        int nSegs;
        long elemSize;
        long dataSize;
        long varOffs;
        boolean isBoolean;

        public Header(ByteBuffer src)
        {
            BigInteger magic = ByteBufferExtensions.getUnsignedLong(src);
            if (magic.equals(RlePayloadMagic))
            {
                log.fine("Magic is payload");
            } else
            {
                log.fine("Magic is shit");
            }

            nSegs = (int) src.getLong();
            elemSize = src.getLong();
            dataSize = src.getLong();
            varOffs = src.getLong();
            isBoolean = src.get() != 0;
            byte[] temp = new byte[7];
            src.get(temp);
        }
    }

    public class Segment
    {
        long pPosition; // position in chunk of first element
        int valueIndex; // index of element in payload array or missing reason
        boolean same; // sequence of same values
        boolean isNull; // trigger if value is NULL (missingReason) or normal
                        // value (valueIndex)

        public Segment(ByteBuffer src)
        {
            pPosition = src.getLong();
            int i = src.getInt();
            valueIndex = i & 0x3fffffff;
            same = (i & 0x40000000) != 0;
            isNull = (i & 0x80000000) != 0;
        }
    }
}