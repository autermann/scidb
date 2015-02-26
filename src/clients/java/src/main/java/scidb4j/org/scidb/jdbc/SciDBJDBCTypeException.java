package org.scidb.jdbc;

import java.sql.SQLException;

public class SciDBJDBCTypeException extends SQLException
{
    private static final long serialVersionUID = 1L;

    /**
     * Construct exception
     * @param fromType Source type
     * @param toType Destination type
     */
    public SciDBJDBCTypeException(String fromType, String toType)
    {
        super(String.format("Can not convert SciDB type '%s' to Java type '%s'", fromType, toType));
    }
}
