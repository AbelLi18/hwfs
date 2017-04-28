package com.abel.hwfs.custom.output;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.lib.db.DBConfiguration;
import org.apache.hadoop.mapreduce.lib.db.DBInputFormat.NullDBWritable;
import org.apache.hadoop.mapreduce.lib.db.DBWritable;

public class MyDBConfiguration {
    /**
     * Number of records to buffer
     * Abel Li added.(2017/1/26)
     */
    public static final String OUTPUT_BUFFER_MAX_PROPERTY =
      "mapreduce.jdbc.output.spill.limit";

    /** The JDBC Driver class name */
    public static final String DRIVER_CLASS_PROPERTY =
      "mapreduce.jdbc.driver.class";

    /** JDBC Database access URL */
    public static final String URL_PROPERTY = "mapreduce.jdbc.url";

    /** User name to access the database */
    public static final String USERNAME_PROPERTY = "mapreduce.jdbc.username";

    /** Password to access the database */
    public static final String PASSWORD_PROPERTY = "mapreduce.jdbc.password";

    /** Input table name */
    public static final String INPUT_TABLE_NAME_PROPERTY =
      "mapreduce.jdbc.input.table.name";

    /** Field names in the Input table */
    public static final String INPUT_FIELD_NAMES_PROPERTY =
      "mapreduce.jdbc.input.field.names";

    /** WHERE clause in the input SELECT statement */
    public static final String INPUT_CONDITIONS_PROPERTY =
      "mapreduce.jdbc.input.conditions";

    /** ORDER BY clause in the input SELECT statement */
    public static final String INPUT_ORDER_BY_PROPERTY =
      "mapreduce.jdbc.input.orderby";

    /** Whole input query, exluding LIMIT...OFFSET */
    public static final String INPUT_QUERY = "mapreduce.jdbc.input.query";

    /** Input query to get the count of records */
    public static final String INPUT_COUNT_QUERY =
      "mapreduce.jdbc.input.count.query";

    /** Input query to get the max and min values of the jdbc.input.query */
    public static final String INPUT_BOUNDING_QUERY =
        "mapred.jdbc.input.bounding.query";

    /** Class name implementing DBWritable which will hold input tuples */
    public static final String INPUT_CLASS_PROPERTY =
      "mapreduce.jdbc.input.class";

    /** Output table name */
    public static final String OUTPUT_TABLE_NAME_PROPERTY =
      "mapreduce.jdbc.output.table.name";

    /** Field names in the Output table */
    public static final String OUTPUT_FIELD_NAMES_PROPERTY =
      "mapreduce.jdbc.output.field.names";

    /** Number of fields in the Output table */
    public static final String OUTPUT_FIELD_COUNT_PROPERTY =
      "mapreduce.jdbc.output.field.count";

    /**
     * Sets the DB access related fields in the {@link Configuration}.
     * @param conf the configuration
     * @param driverClass JDBC Driver class name
     * @param dbUrl JDBC DB access URL.
     * @param userName DB access username
     * @param passwd DB access passwd
     */
    public static void configureDB(Configuration conf, String driverClass,
        String dbUrl, String userName, String passwd) {

      conf.set(DRIVER_CLASS_PROPERTY, driverClass);
      conf.set(URL_PROPERTY, dbUrl);
      if (userName != null) {
        conf.set(USERNAME_PROPERTY, userName);
      }
      if (passwd != null) {
        conf.set(PASSWORD_PROPERTY, passwd);
      }
    }

    /**
     * Sets the DB access related fields in the JobConf.
     * @param job the job
     * @param driverClass JDBC Driver class name
     * @param dbUrl JDBC DB access URL.
     */
    public static void configureDB(Configuration job, String driverClass,
        String dbUrl) {
      configureDB(job, driverClass, dbUrl, null, null);
    }

    private final Configuration conf;

    public MyDBConfiguration(Configuration job) {
      this.conf = job;
    }

    /** Returns a connection object o the DB
     * @throws ClassNotFoundException
     * @throws SQLException */
    public Connection getConnection()
        throws ClassNotFoundException, SQLException {

      Class.forName(conf.get(DBConfiguration.DRIVER_CLASS_PROPERTY));

      if(conf.get(DBConfiguration.USERNAME_PROPERTY) == null) {
        return DriverManager.getConnection(
                 conf.get(DBConfiguration.URL_PROPERTY));
      } else {
        return DriverManager.getConnection(
            conf.get(DBConfiguration.URL_PROPERTY),
            conf.get(DBConfiguration.USERNAME_PROPERTY),
            conf.get(DBConfiguration.PASSWORD_PROPERTY));
      }
    }

    public Configuration getConf() {
      return conf;
    }

    public String getInputTableName() {
      return conf.get(DBConfiguration.INPUT_TABLE_NAME_PROPERTY);
    }

    public void setInputTableName(String tableName) {
      conf.set(DBConfiguration.INPUT_TABLE_NAME_PROPERTY, tableName);
    }

    public String[] getInputFieldNames() {
      return conf.getStrings(DBConfiguration.INPUT_FIELD_NAMES_PROPERTY);
    }

    public void setInputFieldNames(String... fieldNames) {
      conf.setStrings(DBConfiguration.INPUT_FIELD_NAMES_PROPERTY, fieldNames);
    }

    public String getInputConditions() {
      return conf.get(DBConfiguration.INPUT_CONDITIONS_PROPERTY);
    }

    public void setInputConditions(String conditions) {
      if (conditions != null && conditions.length() > 0) {
        conf.set(DBConfiguration.INPUT_CONDITIONS_PROPERTY, conditions);
    }
    }

    public String getInputOrderBy() {
      return conf.get(DBConfiguration.INPUT_ORDER_BY_PROPERTY);
    }

    public void setInputOrderBy(String orderby) {
      if(orderby != null && orderby.length() >0) {
        conf.set(DBConfiguration.INPUT_ORDER_BY_PROPERTY, orderby);
      }
    }

    public String getInputQuery() {
      return conf.get(DBConfiguration.INPUT_QUERY);
    }

    public void setInputQuery(String query) {
      if(query != null && query.length() >0) {
        conf.set(DBConfiguration.INPUT_QUERY, query);
      }
    }

    public String getInputCountQuery() {
      return conf.get(DBConfiguration.INPUT_COUNT_QUERY);
    }

    public void setInputCountQuery(String query) {
      if(query != null && query.length() > 0) {
        conf.set(DBConfiguration.INPUT_COUNT_QUERY, query);
      }
    }

    public void setInputBoundingQuery(String query) {
      if (query != null && query.length() > 0) {
        conf.set(DBConfiguration.INPUT_BOUNDING_QUERY, query);
      }
    }

    public String getInputBoundingQuery() {
      return conf.get(DBConfiguration.INPUT_BOUNDING_QUERY);
    }

    public Class<?> getInputClass() {
      return conf.getClass(DBConfiguration.INPUT_CLASS_PROPERTY,
                           NullDBWritable.class);
    }

    public void setInputClass(Class<? extends DBWritable> inputClass) {
      conf.setClass(DBConfiguration.INPUT_CLASS_PROPERTY, inputClass,
                    DBWritable.class);
    }

    public String getOutputTableName() {
      return conf.get(DBConfiguration.OUTPUT_TABLE_NAME_PROPERTY);
    }

    public void setOutputTableName(String tableName) {
      conf.set(DBConfiguration.OUTPUT_TABLE_NAME_PROPERTY, tableName);
    }

    public String[] getOutputFieldNames() {
      return conf.getStrings(DBConfiguration.OUTPUT_FIELD_NAMES_PROPERTY);
    }

    public void setOutputFieldNames(String... fieldNames) {
      conf.setStrings(DBConfiguration.OUTPUT_FIELD_NAMES_PROPERTY, fieldNames);
    }

    public void setOutputFieldCount(int fieldCount) {
      conf.setInt(DBConfiguration.OUTPUT_FIELD_COUNT_PROPERTY, fieldCount);
    }

    public int getOutputFieldCount() {
      return conf.getInt(OUTPUT_FIELD_COUNT_PROPERTY, 0);
    }

    public int getOutputBufferLimit() {
      return conf.getInt(MyDBConfiguration.OUTPUT_BUFFER_MAX_PROPERTY,
          SetSizeDBOutputFormat.DBRecordWriter.DEFAULT_MAX_BUFFER_SIZE);
    }

    public void setOutputBufferLimit(int limit) {
      conf.setInt(MyDBConfiguration.OUTPUT_BUFFER_MAX_PROPERTY, limit);
    }
}
