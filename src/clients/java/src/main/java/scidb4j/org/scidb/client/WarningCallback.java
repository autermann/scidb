package org.scidb.client;

/**
 * Interface for calling back warning handler
 */
public interface WarningCallback
{
    /**
     * Use this method for handling warnings which server can return during execution
     * @param whatStr Warning string
     */
    void handleWarning(String whatStr);
}

