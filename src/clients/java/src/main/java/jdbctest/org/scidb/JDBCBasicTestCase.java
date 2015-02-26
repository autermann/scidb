package org.scidb;

import junit.framework.TestCase;
import org.scidb.jdbc.ISciDBJDBCResultSetWrapper;

import java.sql.*;

public class JDBCBasicTestCase extends TestCase
{
    private Connection conn;

    public JDBCBasicTestCase(String s)
    {
        super(s);
        try
        {
            Class.forName("org.scidb.jdbc.SciDBJDBCDriver");
        }
        catch (ClassNotFoundException e)
        {
            System.out.println("Driver is not in the CLASSPATH -> " + e);
        }
    }

    public void setUp() throws SQLException {
        conn = DriverManager.getConnection("jdbc:scidb://localhost/");
    }

    public void tearDown() throws SQLException {
        conn.close();
    }

    public void testDDLExecution() throws SQLException
    {
        Statement st = conn.createStatement();
        ResultSet res = st.executeQuery("create array A<i: int64, s: string>[x=0:5,3,0, y=0:9,5,0]");
        assertNull(res);
        conn.commit();


        res = st.executeQuery("drop array A");
        assertNull(res);
        conn.commit();
    }

    public void testSelectiveQuery() throws SQLException
    {
        Statement st = conn.createStatement();
        ResultSet res = st.executeQuery("select * from array(empty<a:string>[x=0:3,2,0], '[(\"1\")(\"\")][(\"2\")]')");
        ResultSetMetaData meta = res.getMetaData();

        assertEquals("build", meta.getTableName(0));
        assertEquals(2, meta.getColumnCount());

        ISciDBJDBCResultSetWrapper resWrapper = res.unwrap(ISciDBJDBCResultSetWrapper.class);
        assertEquals("x", meta.getColumnName(1));
        assertEquals("int64", meta.getColumnTypeName(1));
        assertFalse(resWrapper.isColumnAttribute(1));
        assertEquals("a", meta.getColumnName(2));
        assertEquals("string", meta.getColumnTypeName(2));
        assertTrue(resWrapper.isColumnAttribute(2));

        StringBuilder sb = new StringBuilder();
        while(!res.isAfterLast())
        {
            sb.append(res.getLong("x") + ":" + res.getString("a") + ":");
            res.next();
        }
        assertEquals("0:1:1::2:2:", sb.toString());
    }

    public void testSignedIntegerDataTypes() throws SQLException
    {
        Statement st = conn.createStatement();
        ResultSet res = st.executeQuery("select * from array(empty<i8:int8, i16: int16, i32: int32, i64: int64>" +
                "[x=0:3,2,0], '[(1, 260, 67000, 10000000), (-1, -260, -67000, -10000000)][]')");
        ResultSetMetaData meta = res.getMetaData();

        assertEquals("build", meta.getTableName(0));
        assertEquals(5, meta.getColumnCount());

        ISciDBJDBCResultSetWrapper resWrapper = res.unwrap(ISciDBJDBCResultSetWrapper.class);
        assertEquals("x", meta.getColumnName(1));
        assertEquals("int64", meta.getColumnTypeName(1));
        assertFalse(resWrapper.isColumnAttribute(1));

        assertEquals("i8", meta.getColumnName(2));
        assertEquals("int8", meta.getColumnTypeName(2));
        assertTrue(resWrapper.isColumnAttribute(2));
        assertEquals("i16", meta.getColumnName(3));
        assertEquals("int16", meta.getColumnTypeName(3));
        assertTrue(resWrapper.isColumnAttribute(3));
        assertEquals("i32", meta.getColumnName(4));
        assertEquals("int32", meta.getColumnTypeName(4));
        assertTrue(resWrapper.isColumnAttribute(4));
        assertEquals("i64", meta.getColumnName(5));
        assertEquals("int64", meta.getColumnTypeName(5));
        assertTrue(resWrapper.isColumnAttribute(5));

        StringBuilder sbMain = new StringBuilder();
        StringBuilder sbConvert1 = new StringBuilder();
        StringBuilder sbConvert2 = new StringBuilder();
        StringBuilder sbConvert3 = new StringBuilder();
        StringBuilder sbConvertFloat = new StringBuilder();
        while(!res.isAfterLast())
        {
            sbMain.append(res.getLong("x") + ":" + res.getByte("i8") + ":" + res.getShort("i16") + ":" + res.getInt("i32")
                    + ":" + res.getLong("i64") + ":" + res.getBigDecimal("i64") + ":");
            sbConvert1.append(res.getShort("i8") + ":" + res.getInt("i16") + ":" + res.getLong("i32") + ":"
                    + res.getBigDecimal("i32") + ":");
            sbConvert2.append(res.getInt("i8") + ":" + res.getLong("i16") + ":" + res.getBigDecimal("i16") + ":");
            sbConvert3.append(res.getLong("i8") + ":" + res.getBigDecimal("i8") + ":" + res.getBigDecimal("i16") + ":"
                    + res.getBigDecimal("i32") + ":");
            sbConvertFloat.append(res.getFloat("i8") + ":" + res.getDouble("i8") + ":" +
                    res.getFloat("i16") + ":" + res.getDouble("i16") + ":" +
                    res.getFloat("i32") + ":" + res.getDouble("i32") + ":" +
                    res.getFloat("i64") + ":" + res.getDouble("i64") + ":");
            res.next();
        }
        assertEquals("0:1:260:67000:10000000:10000000:1:-1:-260:-67000:-10000000:-10000000:", sbMain.toString());
        assertEquals("1:260:67000:67000:-1:-260:-67000:-67000:", sbConvert1.toString());
        assertEquals("1:260:260:-1:-260:-260:", sbConvert2.toString());
        assertEquals("1:1:260:67000:-1:-1:-260:-67000:", sbConvert3.toString());
        assertEquals("1.0:1.0:260.0:260.0:67000.0:67000.0:1.0E7:1.0E7:-1.0:-1.0:-260.0:-260.0:-67000.0:-67000.0:-1.0E7:-1.0E7:", sbConvertFloat.toString());
    }

    public void testUnsignedIntegerDataTypes() throws SQLException
    {
        Statement st = conn.createStatement();
        ResultSet res = st.executeQuery("select * from array(empty<i8:uint8, i16: uint16, i32: uint32, i64: uint64>" +
                "[x=0:3,2,0], '[(1, 260, 67000, 10000000)][]')");
        ResultSetMetaData meta = res.getMetaData();

        assertEquals("build", meta.getTableName(0));
        assertEquals(5, meta.getColumnCount());

        ISciDBJDBCResultSetWrapper resWrapper = res.unwrap(ISciDBJDBCResultSetWrapper.class);
        assertEquals("x", meta.getColumnName(1));
        assertEquals("int64", meta.getColumnTypeName(1));
        assertFalse(resWrapper.isColumnAttribute(1));

        assertEquals("i8", meta.getColumnName(2));
        assertEquals("uint8", meta.getColumnTypeName(2));
        assertTrue(resWrapper.isColumnAttribute(2));
        assertEquals("i16", meta.getColumnName(3));
        assertEquals("uint16", meta.getColumnTypeName(3));
        assertTrue(resWrapper.isColumnAttribute(3));
        assertEquals("i32", meta.getColumnName(4));
        assertEquals("uint32", meta.getColumnTypeName(4));
        assertTrue(resWrapper.isColumnAttribute(4));
        assertEquals("i64", meta.getColumnName(5));
        assertEquals("uint64", meta.getColumnTypeName(5));
        assertTrue(resWrapper.isColumnAttribute(5));

        StringBuilder sbMain = new StringBuilder();
        StringBuilder sbConvert1 = new StringBuilder();
        StringBuilder sbConvert2 = new StringBuilder();
        StringBuilder sbConvert3 = new StringBuilder();
        StringBuilder sbConvertFloat = new StringBuilder();
        while(!res.isAfterLast())
        {
            sbMain.append(res.getLong("x") + ":" + res.getShort("i8") + ":" + res.getInt("i16") + ":" + res.getLong("i32")
                    + ":" + res.getBigDecimal("i64") + ":");
            sbConvert1.append(res.getInt("i8") + ":" + res.getLong("i16") + ":" + res.getLong("i32") + ":" + res.getBigDecimal("i32") + ":");
            sbConvert2.append(res.getInt("i8") + ":" + res.getLong("i16") + ":" + res.getBigDecimal("i16") + ":");
            sbConvert3.append(res.getLong("i8") + ":" + res.getBigDecimal("i8") + ":");
            sbConvertFloat.append(res.getFloat("i8") + ":" + res.getDouble("i8") + ":" +
                    res.getFloat("i16") + ":" + res.getDouble("i16") + ":" +
                    res.getFloat("i32") + ":" + res.getDouble("i32") + ":" +
                    res.getFloat("i64") + ":" + res.getDouble("i64") + ":");
            res.next();
        }
        assertEquals("0:1:260:67000:10000000:", sbMain.toString());
        assertEquals("1:260:67000:67000:", sbConvert1.toString());
        assertEquals("1:260:260:", sbConvert2.toString());
        assertEquals("1:1:", sbConvert3.toString());
        assertEquals("1.0:1.0:260.0:260.0:67000.0:67000.0:1.0E7:1.0E7:", sbConvertFloat.toString());
    }

    public void testFloatDataTypes() throws SQLException
    {
        Statement st = conn.createStatement();
        ResultSet res = st.executeQuery("select * from array(empty<f:float, d: double>[x=0:3,2,0], " +
                "'[(3.141592653589793238, 3.141592653589793238)][]')");
        ResultSetMetaData meta = res.getMetaData();

        assertEquals("build", meta.getTableName(0));
        assertEquals(3, meta.getColumnCount());

        ISciDBJDBCResultSetWrapper resWrapper = res.unwrap(ISciDBJDBCResultSetWrapper.class);
        assertEquals("x", meta.getColumnName(1));
        assertEquals("int64", meta.getColumnTypeName(1));
        assertFalse(resWrapper.isColumnAttribute(1));

        assertEquals("f", meta.getColumnName(2));
        assertEquals("float", meta.getColumnTypeName(2));
        assertTrue(resWrapper.isColumnAttribute(2));
        assertEquals("d", meta.getColumnName(3));
        assertEquals("double", meta.getColumnTypeName(3));
        assertTrue(resWrapper.isColumnAttribute(3));

        StringBuilder sbMain = new StringBuilder();
        while(!res.isAfterLast())
        {
            sbMain.append(res.getLong("x") + ":" + res.getFloat("f") + ":" + res.getDouble("d") + ":" +
                    (float)res.getDouble("f") + ":");
            res.next();
        }
        assertEquals("0:3.1415927:3.141592653589793:3.1415927:", sbMain.toString());
    }

    public void testNonEmptyCoordinates() throws SQLException
    {
        Statement st = conn.createStatement();
        ResultSet res = st.executeQuery("select * from array(not empty<a:int32>[x=0:3,2,0], '[10,9][8,7]')");
        ResultSetMetaData meta = res.getMetaData();

        assertEquals("build", meta.getTableName(0));
        assertEquals(2, meta.getColumnCount());

        ISciDBJDBCResultSetWrapper resWrapper = res.unwrap(ISciDBJDBCResultSetWrapper.class);
        assertEquals("x", meta.getColumnName(1));
        assertEquals("int64", meta.getColumnTypeName(1));
        assertFalse(resWrapper.isColumnAttribute(1));

        assertEquals("a", meta.getColumnName(2));
        assertEquals("int32", meta.getColumnTypeName(2));
        assertTrue(resWrapper.isColumnAttribute(2));

        StringBuilder sbMain = new StringBuilder();
        while(!res.isAfterLast())
        {
            sbMain.append(res.getLong("x") + ":" + res.getInt("a") + ",");
            res.next();
        }
        assertEquals("0:10,1:9,2:8,3:7,", sbMain.toString());
    }

    public void testNonEmptyCoordinatesDefaults() throws SQLException
    {
        Statement st = conn.createStatement();
        ResultSet res = st.executeQuery("select * from array(not empty<a:int32>[x=0:3,2,0], '[(10)()][(8)()]')");
        ResultSetMetaData meta = res.getMetaData();

        assertEquals("build", meta.getTableName(0));
        assertEquals(2, meta.getColumnCount());

        ISciDBJDBCResultSetWrapper resWrapper = res.unwrap(ISciDBJDBCResultSetWrapper.class);
        assertEquals("x", meta.getColumnName(1));
        assertEquals("int64", meta.getColumnTypeName(1));
        assertFalse(resWrapper.isColumnAttribute(1));

        assertEquals("a", meta.getColumnName(2));
        assertEquals("int32", meta.getColumnTypeName(2));
        assertTrue(resWrapper.isColumnAttribute(2));

        StringBuilder sbMain = new StringBuilder();
        while(!res.isAfterLast())
        {
            sbMain.append(res.getLong("x") + ":" + res.getInt("a") + ",");
            res.next();
        }
        assertEquals("0:10,1:0,2:8,3:0,", sbMain.toString());
    }

    public void testBooleanTypes() throws SQLException
    {
        Statement st = conn.createStatement();
        ResultSet res = st.executeQuery("select * from array(empty<b:bool>[x=0:3,2,0], '[(true), (false)][(false), (true)]')");
        ResultSetMetaData meta = res.getMetaData();

        assertEquals("build", meta.getTableName(0));
        assertEquals(2, meta.getColumnCount());

        ISciDBJDBCResultSetWrapper resWrapper = res.unwrap(ISciDBJDBCResultSetWrapper.class);
        assertEquals("x", meta.getColumnName(1));
        assertEquals("int64", meta.getColumnTypeName(1));
        assertFalse(resWrapper.isColumnAttribute(1));

        assertEquals("b", meta.getColumnName(2));
        assertEquals("bool", meta.getColumnTypeName(2));
        assertTrue(resWrapper.isColumnAttribute(2));

        StringBuilder sbMain = new StringBuilder();
        while(!res.isAfterLast())
        {
            sbMain.append(res.getLong("x") + ":" + res.getBoolean("b") + ":");
            res.next();
        }
        assertEquals("0:true:1:false:2:false:3:true:", sbMain.toString());
    }
}
