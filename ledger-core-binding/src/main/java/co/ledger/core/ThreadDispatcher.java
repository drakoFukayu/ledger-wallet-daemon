// AUTOGENERATED FILE - DO NOT MODIFY!
// This file generated by Djinni from thread_dispatcher.djinni

package co.ledger.core;

/**Class representing a thread dispatcher */
public abstract class ThreadDispatcher {
    /**
     *Get an execution context where tasks are executed sequentially
     *@param name, string, name of execution context to retrieve
     *@return ExecutionContext object
     */
    public abstract ExecutionContext getSerialExecutionContext(String name);

    /**
     *Get an execution context where tasks are executed in parallel thanks to a thread pool
     *where a system of inter-thread communication was designed
     *@param name, string, name of execution context to retrieve
     *@return ExecutionContext object
     */
    public abstract ExecutionContext getThreadPoolExecutionContext(String name);

    /**
     *Get main execution context (generally where tasks that should never get blocked are executed)
     *@return ExecutionContext object
     */
    public abstract ExecutionContext getMainExecutionContext();

    /**
     *Get lock to handle multithreading
     *@return Lock object
     */
    public abstract Lock newLock();
}
