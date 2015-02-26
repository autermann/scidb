package org.scidb.client;

import org.scidb.io.network.SciDBNetworkMessage.Error;

/**
 * Simple SciDB exception
 */
public class SciDBException extends Exception
{
    private static final long serialVersionUID = 1L;

    /**
     * Construct exception from string
     * @param message String message
     */
    public SciDBException(String message)
    {
        super(message);
    }

    /**
     * Construct exception from network error
     * @param err Network error
     */
    public SciDBException(Error err)
    {
        super(err.getRecord().getWhatStr());
    }
}
