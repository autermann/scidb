package org.scidb.jdbc;

/**
 * Interface which exposes some SciDB's features through JDBC
 */
public interface IStatementWrapper
{
    /**
     * Set query execution mode to AFL or language
     * @param afl true - AFL, false - AQL
     */
    public void setAfl(boolean afl);

    /**
     * Return AFL flag
     * @return true if AFL mode
     */
    public boolean isAfl();

    /**
     * Return AQL flag
     * @return true if AQL mode
     */
    public boolean isAql();
}
