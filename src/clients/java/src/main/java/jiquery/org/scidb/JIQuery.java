package org.scidb;

import java.util.Arrays;

import org.scidb.client.*;

import static java.lang.System.exit;
import static java.lang.System.out;

public class JIQuery
{
    public static void main(String[] args)
    {
        Connection conn = new Connection();
        try
        {
            if (args.length < 1)
            {
                System.err.println("First argument should be query string");
                exit(1);
            }
            String queryString = args[0];
            System.out.printf("Will execute query '%s'\n", queryString);

            conn.setWarningCallback(new WarningCallback()
            {
                @Override
                public void handleWarning(String whatStr)
                {
                    System.out.println("Warning: " + whatStr);
                }
            });
            conn.connect("localhost", 1239);
            SciDBResult res = conn.prepare(queryString);
            Array arr = conn.execute();
            out.println(arr.getSchema().toString());
            arr.fetch();
            while (!arr.getEmptyBitmap().endOfArray())
            {
            	while (!arr.endOfChunk())
                {
                    if (arr.getEmptyBitmap() != null)
                    {
                        System.out.print("(");
                        System.out.print(Arrays.toString(arr.getEmptyBitmap().getCoordinates()));
                        System.out.print(")");
                    }
                    System.out.print("[");
                    System.out.print(arr.getChunk(0).getString());
                    System.out.println("]");
                    arr.move();
                }
                arr.fetch();
            }
        } catch (Exception e)
        {
            e.printStackTrace();
        }
    }
}
