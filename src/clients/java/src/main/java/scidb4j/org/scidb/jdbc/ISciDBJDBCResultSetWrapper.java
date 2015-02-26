package org.scidb.jdbc;

import java.sql.SQLException;

/**
 * Interface which exposes some SciDB's features through JDBC
 */
public interface ISciDBJDBCResultSetWrapper
{
    /**
     * Checks if column derived from array attribute
     * @param columnIndex Column index
     * @return true if attribute
     * @throws SQLException
     */
    public boolean isColumnAttribute(int columnIndex) throws SQLException;

    /**
     * Checks if column derived from array attribute
     * @param columnLabel Column label
     * @return true if attribute
     * @throws SQLException
     */
    public boolean isColumnAttribute(String columnLabel) throws SQLException;

    /**
     * Checks if column derived from array dimension
     * @param columnIndex Column label
     * @return true if dimension
     * @throws SQLException
     */
    public boolean isColumnDimension(int columnIndex) throws SQLException;

    /**
     * Checks if column derived from array dimension
     * @param columnLabel Column label
     * @return true if dimension
     * @throws SQLException
     */
    public boolean isColumnDimension(String columnLabel) throws SQLException;
}
