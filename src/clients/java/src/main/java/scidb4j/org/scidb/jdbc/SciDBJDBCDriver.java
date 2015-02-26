package org.scidb.jdbc;

import java.sql.*;
import java.util.Properties;
import java.util.logging.Logger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class SciDBJDBCDriver implements Driver
{
    public final static String URL_PREFIX = "jdbc:scidb:";
    private static Logger log = Logger.getLogger(org.scidb.client.Connection.class.getName());

    @Override
    public DriverPropertyInfo[] getPropertyInfo(String url, Properties info) throws SQLException
    {
        return new DriverPropertyInfo[0];
    }

    @Override
    public int getMajorVersion()
    {
        return 1;
    }

    @Override
    public int getMinorVersion()
    {
        return 0;
    }

    @Override
    public java.sql.Connection connect(String url, Properties info) throws SQLException
    {
        //Looke like bug in JDBC and this method never used in DriverManager: 
        //http://www.postgresql.org/message-id/4D95DDB8.2080809@ejurka.com
        //So we just call it before connection
        if (!acceptsURL(url))
            return null;
        Pattern pattern = Pattern.compile("^"+URL_PREFIX + "//([\\da-z\\.-]+)(:([0-9]+))?/?" + "$");
        Matcher matcher = pattern.matcher(url);
        if (matcher.find())
        {
            String hostName = matcher.group(1);
            int port = 1239;
            if (matcher.group(3) != null)
            {
                port = Integer.valueOf(matcher.group(3));
            }
            return new SciDBJDBCConnection(hostName, port);
        }
        else
        {
            throw new SQLException("Wrong connection URL, it should be " + URL_PREFIX + "//hostname[:port][/]");
        }
    }
    
    @Override
    public boolean acceptsURL(String url) throws SQLException
    {
        return url.startsWith(URL_PREFIX);
    }

    @Override
    public boolean jdbcCompliant()
    {
        return false;
    }

    static
    {
        try
        {
            java.sql.DriverManager.registerDriver(new SciDBJDBCDriver());
        } catch (SQLException e)
        {
            throw new RuntimeException("FATAL ERROR: Could not initialise SciDB driver! Message was: "
                    + e.getMessage());
        }
    }
}
