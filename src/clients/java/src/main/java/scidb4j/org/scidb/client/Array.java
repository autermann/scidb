package org.scidb.client;

import java.io.IOException;
import java.util.logging.Logger;

import org.scidb.io.network.SciDBNetwork;
import org.scidb.io.network.SciDBNetworkMessage;

/**
 * SciDB client-side array which reads chunks from network
 */
public class Array
{
    private static Logger log = Logger.getLogger(Connection.class.getName());

    private long queryId;
    private Schema schema;
    private SciDBNetwork net;
    private IChunk[] chunks;
    private EmptyChunk _emptyBitmap;

    /**
     * Constructor
     * 
     * @param queryId Query ID
     * @param schema Array schema
     * @param net Network object
     */
    public Array(long queryId, Schema schema, SciDBNetwork net)
    {
        this.queryId = queryId;
        this.schema = schema;
        this.net = net;
        
        chunks = new IChunk[this.schema.getAttributes().length];
    }
    
    /**
     * Returns query ID
     * @return Query ID
     */
    public long getQueryId()
    {
        return queryId;
    }
    
    /**
     * Returns array schema
     * @return Array schema
     */
    public Schema getSchema()
    {
        return schema;
    }
    
    /**
     * Fetch new chunks for each attribute
     * @throws IOException
     * @throws SciDBException
     */
    public void fetch() throws IOException, SciDBException
    {
        log.fine(String.format("Fetching chunks"));
        for (Schema.Attribute att: schema.getAttributes())
        {
            SciDBNetworkMessage msg = new SciDBNetworkMessage.Fetch(queryId, att.getId(), schema.getName());
            net.write(msg);
            msg = net.read();

            if (!att.isEmptyIndicator())
            {
                chunks[att.getId()] = (IChunk) new Chunk((SciDBNetworkMessage.Chunk) msg, this);
            } else
            {
                _emptyBitmap = new EmptyChunk((SciDBNetworkMessage.Chunk) msg, this);
                chunks[att.getId()] = (IChunk) _emptyBitmap;
            }
        }
    }

    /**
     * Move current items in current chunks
     * @return true if move was successful.
     */
    public boolean move()
    {
        boolean result = true;
        for (IChunk c: chunks)
        {
            result = result && c.move();
        }
        return result;
    }

    /**
     * Check if there is no more item in current chunk
     * @return
     */
    public boolean endOfChunk()
    {
        boolean result = false;
        for (IChunk c: chunks)
        {
            result = result || c.endOfChunk();
        }
        return result;
    }

    public long[] getCoordinates()
    {
        if (_emptyBitmap != null)
        {
            return _emptyBitmap.getCoordinates();
        }
        else
        {
            return chunks[0].getCoordinates();
        }
    }

    /**
     * Returns chunk of specified attribute
     * @param attributeId Attribute ID
     * @return Array chunk
     */
    public Chunk getChunk(int attributeId)
    {
        if (!schema.getAttributes()[attributeId].isEmptyIndicator())
        {
            return (Chunk) chunks[attributeId];
        } else
        {
            return null;
        }
    }

    /**
     * @return empty bitmap chunk to iterate over non empty cells and know their coordinates.
     */
    public EmptyChunk getEmptyBitmap()
    {
        return _emptyBitmap;
    }
}
