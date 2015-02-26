package org.scidb.io.network;

import java.io.IOException;
import java.net.Socket;
import java.net.SocketException;

import org.scidb.client.SciDBException;

/**
 * Class for handling reading and writing arbitrary network messages syncronously
 */
public class SciDBNetwork
{
    private Socket sock;

    /**
     * Connect to server
     * 
     * @param host Host name or IP
     * @param port Port number
     * @throws IOException
     */
    public void connect(String host, int port) throws IOException
    {
        sock = new Socket(host, port);
    }

    /**
     * Close connection
     * 
     * @throws IOException
     */
    public void disconnect() throws IOException
    {
        sock.close();
        sock = null;
    }

    /**
     * Set timeout to socket
     * 
     * @param timeout Timeout im milliseconds
     * @throws SocketException
     */
    public void setTimeout(int timeout) throws SocketException
    {
        sock.setSoTimeout(timeout);
    }

    /**
     * Returns timeout of socket in milliseconds
     * @return Timeout
     * @throws SocketException
     */
    public int getTimeout() throws SocketException
    {
        return sock.getSoTimeout();
    }

    /**
     * Write arbitrary SciDB network message to socket
     * 
     * @param msg Message object
     * @throws IOException
     */
    public void write(SciDBNetworkMessage msg) throws IOException
    {
        msg.writeToStream(sock.getOutputStream());
    }

    /**
     * Read arbitrary network message from socket
     * 
     * @return SciDB network message
     * @throws SciDBException
     * @throws IOException
     */
    public SciDBNetworkMessage read() throws SciDBException, IOException
    {
        return SciDBNetworkMessage.parseFromStream(sock.getInputStream());
    }

    /**
     * Check if connected to server
     * @return true if connected
     */
    public boolean isConnected()
    {
        return sock != null && sock.isConnected();
    }
}
