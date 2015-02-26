package org.scidb.client;

import org.scidb.io.network.SciDBNetworkMessage.QueryResult;
import org.scidb.io.network.ScidbMsg;

/**
 * Query result
 */
public class SciDBResult
{
    private Schema schema;
    private long queryId;
    private boolean selective;
    private String explainLogical;
    private String explainPhysical;

    /**
     * Constructor
     * @param result Query result network message
     * @param conn Connection
     */
    public SciDBResult(QueryResult result, Connection conn)
    {
        ScidbMsg.QueryResult rec = result.getRecord();
        String schemaName = rec.getArrayName();
        Schema.Attribute[] attributes = new Schema.Attribute[rec.getAttributesCount()];
        Schema.Dimension[] dimensions = new Schema.Dimension[rec.getDimensionsCount()];

        int i = 0;
        for (ScidbMsg.QueryResult.AttributeDesc att : rec.getAttributesList())
        {
            attributes[i] = new Schema.Attribute(att.getId(), att.getName(), att.getType(), att
                    .getFlags());
            i++;
        }

        i = 0;
        for (ScidbMsg.QueryResult.DimensionDesc dim : rec.getDimensionsList())
        {
            dimensions[i] = new Schema.Dimension(dim.getName(), dim.getTypeId(), dim.getFlags(), dim.getStartMin(),
                    dim.getCurrStart(), dim.getCurrEnd(), dim.getEndMax(), dim.getChunkInterval());
            i++;
        }

        this.queryId = result.getHeader().queryID;
        this.schema = new Schema(schemaName, attributes, dimensions);
        this.selective = rec.getSelective();
        this.explainLogical = rec.getExplainLogical();
        this.explainPhysical = rec.getExplainPhysical();
        
        if (rec.getWarningsCount() > 0 && conn.getWarningCallback() != null)
        {
            for (ScidbMsg.QueryResult.Warning warn : rec.getWarningsList())
            {
                conn.getWarningCallback().handleWarning(warn.getWhatStr());
            }
        }
    }

    /**
     * Returns result schema
     * @return Schema
     */
    public Schema getSchema()
    {
        return schema;
    }

    /**
     * Returns result query ID
     * @return Query ID
     */
    public long getQueryId()
    {
        return queryId;
    }
    
    /**
     * Returns selective flag
     * @return true - if selective
     */
    public boolean isSelective()
    {
        return selective;
    }
    
    /**
     * Returns explained logical plan
     * @return Logical plan
     */
    public String getExplainLogical()
    {
        return explainLogical;
    }
    
    /**
     * Returns explained physical plan
     * @return Physical plan
     */
    public String getExplainPhysical()
    {
        return explainPhysical;
    }
}
