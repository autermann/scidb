package org.scidb.jdbc;


public class StatementWrapper implements IStatementWrapper
{
    org.scidb.client.Connection _sciDbConnection;

    StatementWrapper(org.scidb.client.Connection sciDbConnection)
    {
        _sciDbConnection = sciDbConnection;
    }

    @Override
    public void setAfl(boolean afl)
    {
        _sciDbConnection.setAfl(afl);
    }

    @Override
    public boolean isAfl()
    {
        return _sciDbConnection.isAfl();
    }

    @Override
    public boolean isAql()
    {
        return _sciDbConnection.isAql();
    }
}
