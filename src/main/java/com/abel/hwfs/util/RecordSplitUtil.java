package com.abel.hwfs.util;

import java.util.Date;

import org.apache.hadoop.io.Text;
import org.apache.log4j.Logger;

import com.abel.hwfs.common.Constants;

public class RecordSplitUtil {

    private static Logger log = Logger.getLogger(RecordSplitUtil.class);

    private final String [] values;

    public RecordSplitUtil(Text record) {
        this.values = (record == null ? Constants.TAB_FIVE : record.toString()).split(Constants.TAB);
    }

    public String[] getValues() {
        return values;
    }

    public String getSearchUserId() {
        return values[0];
    }

    public String getProvince() {
        return values[0];
    }

    public String getContext() {
        return values[1];
    }

    public String getKeyword() {
        return values[2];
    }

    public Date getSearchDate() {
        return SwapDateAndStringUtil.StrToDate(values[3]);
    }

    public int getCount() {
        int count = 0;
        try {
            count = Integer.parseInt(values[4]);
        } catch (Exception e) {
            e.printStackTrace();
            log.error(e.getMessage());
        }
        return count;
    }

    public int getRepeatSearchCountOneDay() {
        int repeatSearchCountOneDay = 0;
        try {
            repeatSearchCountOneDay = Integer.parseInt(values[5]);
        } catch (Exception e) {
            e.printStackTrace();
            log.error(e.getMessage());
        }
        return repeatSearchCountOneDay;
    }
}
