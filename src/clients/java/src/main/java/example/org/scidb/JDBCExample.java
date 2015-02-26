package org.scidb;

import org.scidb.jdbc.ISciDBJDBCResultSetWrapper;

import java.io.IOException;
import java.sql.DriverManager;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;

class JDBCExample
{ 
    public static void main(String [] args) throws IOException
    {
        try
        {
            Class.forName("org.scidb.jdbc.SciDBJDBCDriver");
        }
        catch (ClassNotFoundException e)
        {
            System.out.println("Driver is not in the CLASSPATH -> " + e);
        }

        try
        {
            Connection conn = DriverManager.getConnection("jdbc:scidb://localhost/");
            Statement st = conn.createStatement();
            //create array A<a:string>[x=0:2,3,0, y=0:2,3,0];
            //select * into A from array(A, '[["a","b","c"]["d","e","f"]["123","456","789"]]');
            ResultSet res = st.executeQuery("select * from array(<a:string>[x=0:2,3,0, y=0:2,3,0], '[[\"a\",\"b\",\"c\"][\"d\",\"e\",\"f\"][\"123\",\"456\",\"789\"]]')");
            ResultSetMetaData meta = res.getMetaData();

            System.out.println("Source array name: " + meta.getTableName(0));
            System.out.println(meta.getColumnCount() + " columns:");

            ISciDBJDBCResultSetWrapper resWrapper = res.unwrap(ISciDBJDBCResultSetWrapper.class);
            for (int i = 0; i < meta.getColumnCount(); i++)
            {
                System.out.println(meta.getColumnName(i) + " - " + meta.getColumnTypeName(i) + " - is attribute:" + resWrapper.isColumnAttribute(i));
            }
            System.out.println("=====");

            System.out.println("x y a");
            System.out.println("-----");
            while(!res.isAfterLast())
            {
                System.out.println(res.getLong("x") + " " + res.getLong("y") + " " + res.getString("a"));
                res.next();
            }
        }
        catch (SQLException e)
        {
            System.out.println(e);
        }
    	
    	System.exit(0);
    }
}
