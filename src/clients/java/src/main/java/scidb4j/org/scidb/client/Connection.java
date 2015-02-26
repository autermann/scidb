package org.scidb.client;

import java.io.IOException;
import java.net.SocketException;
import java.util.logging.*;

import org.scidb.io.network.SciDBNetwork;
import org.scidb.io.network.SciDBNetworkMessage;
import org.scidb.io.network.SciDBNetworkMessage.Error;
import org.scidb.io.network.SciDBNetworkMessage.QueryResult;

/**
 * SciDB connection
 */
public class Connection
{
    private SciDBNetwork net;
    private boolean afl = false;
    private long queryId = 0;
    private WarningCallback warningCallback;

    private static Logger log = Logger.getLogger(Connection.class.getName());

    /**
     * Constructor
     */
    public Connection()
    {
        net = new SciDBNetwork();
    }

    /**
     * Connect to specified SciDB instance
     * @param host Host name
     * @param port Port number
     * @throws SciDBException
     * @throws IOException
     */
    public void connect(String host, int port) throws SciDBException, IOException
    {
        net.connect(host, port);
    }

    /**
     * Close network connection
     * @throws IOException
     */
    public void close() throws IOException
    {
        net.disconnect();
    }

    /**
     * Check if connected to server
     * @return true if connected
     */
    public boolean connected()
    {
        return net.isConnected();
    }
    
    /**
     * Prepare query
     * @param queryString Query string
     * @return Result with prepared query ID
     * @throws SciDBException
     * @throws IOException
     */
    public SciDBResult prepare(String queryString) throws SciDBException, IOException
    {
        log.fine(String.format("Preparing query '%s'", queryString));
        SciDBNetworkMessage msg = new SciDBNetworkMessage.Query(0, queryString, afl, "", false);
        net.write(msg);
        msg = net.read();

        switch (msg.getHeader().messageType)
        {
            case SciDBNetworkMessage.mtQueryResult:
                log.fine("Got result from server");
                SciDBResult res = new SciDBResult((QueryResult) msg, this);
                queryId = res.getQueryId(); 
                return res;

            case SciDBNetworkMessage.mtError:
                log.fine("Got error message from server");
                throw new SciDBException((Error) msg);

            default:
                log.severe("Got unhandled network message during execution");
                throw new SciDBException(String.format("Can not handle network message '%s'",
                        msg.getHeader().messageType));
        }
    }
    
    /**
     * Set query execution mode to AFL or language
     * @param afl true - AFL, false - AQL
     */
    public void setAfl(boolean afl)
    {
        this.afl = afl;
    }

    /**
     * Return AFL flag
     * @return true if AFL mode
     */
    public boolean isAfl()
    {
        return afl;
    }
    
    /**
     * Return AQL flag
     * @return true if AQL mode
     */
    public boolean isAQL()
    {
        return !afl;
    }
    
    /**
     * Execute prepared query
     * @return Array result
     * @throws IOException
     * @throws SciDBException
     */
    public Array execute() throws IOException, SciDBException
    {
        if (queryId == 0)
        {
            throw new SciDBException("Query not prepared");
        }
        
        log.fine(String.format("Executing query"));
        SciDBNetworkMessage msg = new SciDBNetworkMessage.Query(queryId, "", afl, "", true);
        net.write(msg);
        msg = net.read();

        switch (msg.getHeader().messageType)
        {
            case SciDBNetworkMessage.mtQueryResult:
                log.fine("Got result from server");
                SciDBResult res = new SciDBResult((QueryResult) msg, this);
                if (res.isSelective())
                    return new Array(res.getQueryId(), res.getSchema(), net);
                else
                    return null;

            case SciDBNetworkMessage.mtError:
                log.fine("Got error message from server");
                throw new SciDBException((Error) msg);

            default:
                log.severe("Got unhandled network message during execution");
                throw new SciDBException(String.format("Can not handle network message '%s'",
                        msg.getHeader().messageType));
        }
    }
    
    /**
     * Commit query
     */
    public void commit() throws IOException, SciDBException
    {
        net.write(new SciDBNetworkMessage.CompleteQuery(queryId));
        SciDBNetworkMessage msg = net.read();

        switch (msg.getHeader().messageType)
        {
            case SciDBNetworkMessage.mtError:
                Error err = (Error) msg;
                if (err.getRecord().getLongErrorCode() != 0)
                {
                    log.fine("Got error message from server");
                    throw new SciDBException((Error) msg);
                }
                log.fine("Query completed successfully");
                break;

            default:
                log.severe("Got unhandled network message during query completing");
                throw new SciDBException(String.format("Can not handle network message '%s'",
                        msg.getHeader().messageType));
        }
    }
    
    /**
     * Rollback query
     */
    public void rollback() throws IOException, SciDBException
    {
        net.write(new SciDBNetworkMessage.AbortQuery(queryId));
        SciDBNetworkMessage msg = net.read();

        switch (msg.getHeader().messageType)
        {
            case SciDBNetworkMessage.mtError:
                Error err = (Error) msg;
                if (err.getRecord().getLongErrorCode() != 0)
                {
                    log.fine("Got error message from server");
                    throw new SciDBException((Error) msg);
                }
                log.fine("Query aborted successfully");
                break;

            default:
                log.severe("Got unhandled network message during query aborting");
                throw new SciDBException(String.format("Can not handle network message '%s'",
                        msg.getHeader().messageType));
        }
    }
    
    /**
     * Set warning callback for registering execution warnings
     * @param callback Callback object
     */
    public void setWarningCallback(WarningCallback callback)
    {
        warningCallback = callback;
    }
    
    /**
     * Returns warning callback
     * @return Callback object
     */
    public WarningCallback getWarningCallback()
    {
        return warningCallback;
    }

    public void setTimeout(int timeout) throws SocketException
    {
        net.setTimeout(timeout);
    }

    public int getTimeout() throws SocketException
    {
        return net.getTimeout();
    }
}
