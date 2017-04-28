package com.abel.hwfs.custom.output;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Date;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.lib.db.DBWritable;

import com.abel.hwfs.util.RecordSplitUtil;

public class WriteDataToDB {

    public static class WordDetailTableWritable implements Writable, DBWritable {

        private String province;
        private String context;
        private String keyword;
        private Date searchDate;
        private int count;
        private int repeatSearchCountOneDay;

        public WordDetailTableWritable(Text record) {
            super();
            RecordSplitUtil values = new RecordSplitUtil(record);
            this.province = values.getProvince();
            this.context = values.getContext();
            this.keyword = values.getKeyword();
            this.searchDate = values.getSearchDate();
            this.count = values.getCount();
            this.repeatSearchCountOneDay = values.getRepeatSearchCountOneDay();
        }

        public void write(PreparedStatement statement) throws SQLException {
            statement.setString(1, this.province);
            statement.setString(2, this.context);
            statement.setString(3, this.keyword);
            statement.setDate(4, new java.sql.Date(this.searchDate.getTime()));
            statement.setInt(5, this.count);
            statement.setInt(6, this.repeatSearchCountOneDay);

        }

        public void readFields(ResultSet resultSet) throws SQLException {
            this.province = resultSet.getString(1);
            this.context = resultSet.getString(2);
            this.keyword = resultSet.getString(3);
            this.searchDate = resultSet.getDate(4);
            this.count = resultSet.getInt(5);
            this.repeatSearchCountOneDay = resultSet.getInt(6);
        }

        public void write(DataOutput out) throws IOException {
        }

        public void readFields(DataInput in) throws IOException {
        }

    }
}
