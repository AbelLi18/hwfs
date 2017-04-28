package com.abel.hwfs.custom.output;

/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.OutputCommitter;
import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.db.DBWritable;
import org.apache.hadoop.mapreduce.lib.output.FileOutputCommitter;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.StringUtils;

/**
 * A OutputFormat that sends the reduce output to a SQL table.
 * <p>
 * {@link MyDBOutputFormat} accepts &lt;key,value&gt; pairs, where
 * key has a type extending DBWritable. Returned {@link RecordWriter}
 * writes <b>only the key</b> to the database with a batch SQL query.
 *
 */
@InterfaceAudience.Public
@InterfaceStability.Stable
public class SetSizeDBOutputFormat<K  extends DBWritable, V>
extends OutputFormat<K,V> {

  private static final Log LOG = LogFactory.getLog(SetSizeDBOutputFormat.class);
  @Override
    public void checkOutputSpecs(JobContext context)
          throws IOException, InterruptedException {}

      @Override
    public OutputCommitter getOutputCommitter(TaskAttemptContext context)
          throws IOException, InterruptedException {
        return new FileOutputCommitter(FileOutputFormat.getOutputPath(context),
                                       context);
    }

  /**
   * A RecordWriter that writes the reduce output to a SQL table
   */
  @InterfaceStability.Evolving
  public class DBRecordWriter
      extends RecordWriter<K, V> {

    private Connection connection;
    private PreparedStatement statement;

    /**
     * By default, buffer spills are disabled; everything is spilled
     * only in close().
     */
    static final int DEFAULT_MAX_BUFFER_SIZE = 1024;

    /** Number of statements in the current execution batch. */
    private int numBufferedStatements;

    /**
     * Number of statements after which a db spill will be triggered.
     * Set to zero to disable intermediate spills.
     */
    private int maxBufferedStatements;

    public DBRecordWriter() throws SQLException {
        this(null, null);
    }

    public DBRecordWriter(Connection connection
        , PreparedStatement statement) throws SQLException {
        this(connection, statement, DEFAULT_MAX_BUFFER_SIZE);
    }

    public DBRecordWriter(Connection connection
        , PreparedStatement statement, int maxBuffer) throws SQLException {
        this.connection = connection;
        this.statement = statement;
        if (null != this.connection) {
          this.connection.setAutoCommit(false);
        }
        this.numBufferedStatements = 0;

        if (maxBuffer < 0) {
          this.maxBufferedStatements = DEFAULT_MAX_BUFFER_SIZE;
          LOG.warn("Max buffer size of " + maxBuffer + " is invalid; setting to "
              + DEFAULT_MAX_BUFFER_SIZE);
        } else {
          this.maxBufferedStatements = maxBuffer;
        }
    }

    public Connection getConnection() {
      return connection;
    }

    public PreparedStatement getStatement() {
      return statement;
    }

    /**
     * Cause the current buffer of prepared statements to be sent
     * to the database.
     */
    protected void spill() throws IOException {
        LOG.debug("Spilling " + this.numBufferedStatements
                + " records to database.");
        try {
            statement.executeBatch();
            connection.commit();
            statement.clearBatch();
            numBufferedStatements = 0;
        } catch (SQLException e) {
            try {
                connection.rollback();
            }
              catch (SQLException ex) {
                LOG.warn(StringUtils.stringifyException(ex));
            }
            throw new IOException(e.getMessage());
        }
        LOG.debug("Spill complete.");
    }

    /** Trigger a spill if our buffer is oversize. */
    protected void maybeSpill() throws IOException {
      if (maxBufferedStatements != 0
          && numBufferedStatements > maxBufferedStatements) {
        spill();
      }
    }

    /** {@inheritDoc} */
    @Override
    public void close(TaskAttemptContext context) throws IOException {
     try {
       spill();
     } finally {
         try {
             statement.close();
             connection.close();
         } catch (SQLException ex) {
             throw new IOException(ex.getMessage());
         }
     }
    }

    /** {@inheritDoc} */
    @Override
    public void write(K key, V value) throws IOException {
      try {
        key.write(statement);
        statement.addBatch();
        numBufferedStatements++;
        maybeSpill();
      } catch (SQLException e) {
        e.printStackTrace();
      }
    }
  }

  /**
   * Constructs the query used as the prepared statement to insert data.
   *
   * @param table
   *          the table to insert into
   * @param fieldNames
   *          the fields to insert into. If field names are unknown, supply an
   *          array of nulls.
   */
  public String constructQuery(String table, String[] fieldNames) {
    if(fieldNames == null) {
      throw new IllegalArgumentException("Field names may not be null");
    }

    StringBuilder query = new StringBuilder();
    query.append("INSERT INTO ").append(table);

    if (fieldNames.length > 0 && fieldNames[0] != null) {
      query.append(" (");
      for (int i = 0; i < fieldNames.length; i++) {
        query.append(fieldNames[i]);
        if (i != fieldNames.length - 1) {
          query.append(",");
        }
      }
      query.append(")");
    }
    query.append(" VALUES (");

    for (int i = 0; i < fieldNames.length; i++) {
      query.append("?");
      if(i != fieldNames.length - 1) {
        query.append(",");
      }
    }
    query.append(");");

    return query.toString();
  }

  /** {@inheritDoc} */
  @Override
public RecordWriter<K, V> getRecordWriter(TaskAttemptContext context)
      throws IOException {
    MyDBConfiguration dbConf = new MyDBConfiguration(context.getConfiguration());
    String tableName = dbConf.getOutputTableName();
    String[] fieldNames = dbConf.getOutputFieldNames();
    int maxBufferSize = dbConf.getOutputBufferLimit();

    if(fieldNames == null) {
      fieldNames = new String[dbConf.getOutputFieldCount()];
    }

    try {
      Connection connection = dbConf.getConnection();
      PreparedStatement statement = null;

      statement = connection.prepareStatement(
                    constructQuery(tableName, fieldNames));
      return new DBRecordWriter(connection, statement, maxBufferSize);
    } catch (Exception ex) {
      throw new IOException(ex.getMessage());
    }
  }

  /**
   * Initializes the reduce-part of the job with
   * the appropriate output settings
   *
   * @param job The job
   * @param tableName The table to insert data into
   * @param fieldNames The field names in the table.
   */
  public static void setOutput(Job job, String tableName,
      String... fieldNames) throws IOException {
    if(fieldNames.length > 0 && fieldNames[0] != null) {
      MyDBConfiguration dbConf = setOutput(job, tableName);
      dbConf.setOutputFieldNames(fieldNames);
    } else {
      if (fieldNames.length > 0) {
        setOutput(job, tableName, fieldNames.length);
      }
      else {
        throw new IllegalArgumentException(
          "Field names must be greater than 0");
      }
    }
  }

  /**
   * Initializes the reduce-part of the job
   * with the appropriate output settings
   *
   * @param job The job
   * @param tableName The table to insert data into
   * @param fieldCount the number of fields in the table.
   */
  public static void setOutput(Job job, String tableName,
      int fieldCount) throws IOException {
      MyDBConfiguration dbConf = setOutput(job, tableName);
    dbConf.setOutputFieldCount(fieldCount);
  }

  private static MyDBConfiguration setOutput(Job job,
      String tableName) throws IOException {
    job.setOutputFormatClass(SetSizeDBOutputFormat.class);
    job.setReduceSpeculativeExecution(false);

    MyDBConfiguration dbConf = new MyDBConfiguration(job.getConfiguration());

    dbConf.setOutputTableName(tableName);
    return dbConf;
  }
}
