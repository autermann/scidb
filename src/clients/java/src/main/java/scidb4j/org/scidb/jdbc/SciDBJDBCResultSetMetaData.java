package org.scidb.jdbc;

import org.scidb.client.Schema;
import org.scidb.client.Schema.Attribute;
import org.scidb.client.Schema.Dimension;

import java.sql.ResultSetMetaData;
import java.sql.SQLException;

public class SciDBJDBCResultSetMetaData implements ResultSetMetaData
{
    private Schema schema;
    private String[] columnsNames;
    private String[] columnsTypes;
    private int columnsCount;

    public SciDBJDBCResultSetMetaData(Schema schema)
    {
        this.schema = schema;
        columnsCount = schema.getDimensions().length + schema.getAttributes().length - (schema.getEmptyIndicator() != null ? 1 : 0);
        columnsNames = new String[columnsCount];
        columnsTypes = new String[columnsCount];

        int i = 0;
        for (Dimension dim: schema.getDimensions())
        {
            columnsNames[i] = dim.getName();
            columnsTypes[i] = dim.getType();
            i++;
        }

        for (Attribute att: schema.getAttributes())
        {
            if (schema.getEmptyIndicator() != null && att.getId() == schema.getEmptyIndicator().getId())
                continue;
            columnsNames[i] = att.getName();
            columnsTypes[i] = att.getType();
            i++;
        }
    }
    
    @Override
    public <T> T unwrap(Class<T> iface) throws SQLException
    {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public boolean isWrapperFor(Class<?> iface) throws SQLException
    {
        // TODO Auto-generated method stub
        return false;
    }

    @Override
    public int getColumnCount() throws SQLException
    {
        return columnsCount;
    }

    @Override
    public boolean isAutoIncrement(int column) throws SQLException
    {
        // TODO Auto-generated method stub
        return false;
    }

    @Override
    public boolean isCaseSensitive(int column) throws SQLException
    {
        // TODO Auto-generated method stub
        return false;
    }

    @Override
    public boolean isSearchable(int column) throws SQLException
    {
        // TODO Auto-generated method stub
        return false;
    }

    @Override
    public boolean isCurrency(int column) throws SQLException
    {
        // TODO Auto-generated method stub
        return false;
    }

    @Override
    public int isNullable(int column) throws SQLException
    {
        // TODO Auto-generated method stub
        return 0;
    }

    @Override
    public boolean isSigned(int column) throws SQLException
    {
        // TODO Auto-generated method stub
        return false;
    }

    @Override
    public int getColumnDisplaySize(int column) throws SQLException
    {
        // TODO Auto-generated method stub
        return 0;
    }

    @Override
    public String getColumnLabel(int column) throws SQLException
    {
        return columnsNames[column - 1];
    }

    @Override
    public String getColumnName(int column) throws SQLException
    {
        return columnsNames[column - 1];
    }

    @Override
    public String getSchemaName(int column) throws SQLException
    {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public int getPrecision(int column) throws SQLException
    {
        // TODO Auto-generated method stub
        return 0;
    }

    @Override
    public int getScale(int column) throws SQLException
    {
        // TODO Auto-generated method stub
        return 0;
    }

    @Override
    public String getTableName(int column) throws SQLException
    {
        return schema.getName();
    }

    @Override
    public String getCatalogName(int column) throws SQLException
    {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public int getColumnType(int column) throws SQLException
    {
        return 0;
    }

    @Override
    public String getColumnTypeName(int column) throws SQLException
    {
        return columnsTypes[column - 1];
    }

    @Override
    public boolean isReadOnly(int column) throws SQLException
    {
        return true;
    }

    @Override
    public boolean isWritable(int column) throws SQLException
    {
        return false;
    }

    @Override
    public boolean isDefinitelyWritable(int column) throws SQLException
    {
        return false;
    }

    @Override
    public String getColumnClassName(int column) throws SQLException
    {
        return null;
    }

}
