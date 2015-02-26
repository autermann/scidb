package org.scidb.client;

public interface IChunk
{
    public int getAttributeId();

    /**
     * @return true if it's eof flag and there is no data in it. Access data
     *         causes an errors.
     */
    public boolean endOfArray();

    public boolean endOfChunk();

    /**
     * Check if current item is not last
     *
     * @return true if the next call of move return true and the next item
     *         exists
     */
    public boolean hasNext();

    /**
     * move current item forward. The first item must be available always.
     *
     * @return true if move is success and new item can be read. false is there
     *         is no new item. do not call getXXX method in this case.
     */
    public boolean move();

    public long getArrayId();

    public long[] getCoordinates();
}
