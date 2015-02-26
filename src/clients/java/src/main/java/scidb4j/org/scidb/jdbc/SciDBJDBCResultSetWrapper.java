package org.scidb.jdbc;

import java.sql.SQLException;

public class SciDBJDBCResultSetWrapper implements ISciDBJDBCResultSetWrapper
{
    SciDBJDBCResultSet result;

    public SciDBJDBCResultSetWrapper(SciDBJDBCResultSet result)
    {
        this.result = result;
    }

    @Override
    public boolean isColumnAttribute(int columnIndex) throws SQLException
    {
        return result.isAttribute(columnIndex);
    }

    @Override
    public boolean isColumnAttribute(String columnLabel) throws SQLException
    {
        return result.isAttribute(result.findColumn(columnLabel));
    }

    @Override
    public boolean isColumnDimension(int columnIndex) throws SQLException
    {
        return !isColumnAttribute(columnIndex);
    }

    @Override
    public boolean isColumnDimension(String columnLabel) throws SQLException
    {
        return !isColumnAttribute(columnLabel);
    }
}
