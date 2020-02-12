using log4net.Appender;
using log4net.Core;
using log4net.Util;
using System.Data;

namespace postgreslog
{
    /// <summary>
    /// This class is responsible for logging out the application log messages to Postgres DB.
    /// This is a possible workaround for a JIRA issue:
    /// https://issues.apache.org/jira/browse/LOG4NET-538
    /// Please use this package at your own risk.
    /// </summary>
    public class PostgresAppender : AdoNetAppender
    {
        // protected virtual void SendBuffer(IDbTransaction dbTran, LoggingEvent[] events)
        /// <summary>
        /// This is the main change for working with postgres DB and Ado.NET Appender and log4net 2.0.8 for .NET core
        /// </summary>
        /// <param name="dbTran">The DB transaction for logging</param>
        /// <param name="events">The events to be saved to DB.</param>
        protected override void SendBuffer(IDbTransaction dbTran, LoggingEvent[] events)
        {
            // string.IsNotNullOrWhiteSpace() does not exist in ancient .NET frameworks
            if (!string.IsNullOrWhiteSpace(CommandText))
            {
                using (IDbCommand dbCmd = Connection.CreateCommand())
                {
                    // Set the command string
                    dbCmd.CommandText = CommandText;

                    // Set the command type
                    dbCmd.CommandType = CommandType;
                    // Send buffer using the prepared command object
                    if (dbTran != null)
                    {
                        dbCmd.Transaction = dbTran;
                    }

                    // clear parameters that have been set
                    dbCmd.Parameters.Clear();

                    // Set the parameter values
                    foreach (AdoNetAppenderParameter param in m_parameters)
                    {
                        param.Prepare(dbCmd);
                    }

                    // prepare the command, which is significantly faster
                    dbCmd.Prepare();

                    // run for all events
                    foreach (LoggingEvent e in events)
                    {
                        // Set the parameter values
                        foreach (AdoNetAppenderParameter param in m_parameters)
                        {
                            param.FormatValue(dbCmd, e);
                        }

                        // Execute the query
                        // LogLog.Debug(System.Reflection.MethodBase.GetCurrentMethod().DeclaringType, dbCmd.ToString());
                        dbCmd.ExecuteNonQuery();
                    }
                }
            }
            else
            {
                // create a new command
                using (IDbCommand dbCmd = Connection.CreateCommand())
                {
                    if (dbTran != null)
                    {
                        dbCmd.Transaction = dbTran;
                    }
                    // run for all events
                    foreach (LoggingEvent e in events)
                    {
                        // Get the command text from the Layout
                        string logStatement = GetLogStatement(e);
                        dbCmd.CommandText = logStatement;
                        dbCmd.ExecuteNonQuery();
                    }
                }
            }
        }
    }
}
