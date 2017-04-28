package com.abel.hwfs.util;
import org.apache.hadoop.io.Text;
import org.apache.log4j.Logger;

import com.abel.hwfs.common.Constants;

public class MapperOutUtil {

    private static Logger log = Logger.getLogger(MapperOutUtil.class);

    private final String [] values;
    private final String recordDate;
    private final String fileName;

    public String getRecordDate() {
        return recordDate;
    }

    public String getContext() {
        String context;
        try {
            context = values[1].substring(1, values[1].length() - 1);
        } catch (Exception e) {
            log.error(e.getMessage());
            context = Constants.EmptyString;
        }
        return context;
    }

    public MapperOutUtil(Text record, String fileName) {
        super();
        this.fileName = fileName;
        this.values = (record == null ? Constants.TAB_FIVE : record.toString()).split(Constants.TAB);
        // file name format: filename.yyyyMMdd.decode.filter
        this.recordDate = fileName.split(Constants.DOT_SPLIT)[1];
    }

    public Text getMapperValueOutput(String keyword) {
        return new Text(values[0] + Constants.TAB
                        + getContext() + Constants.TAB
                        + keyword + Constants.TAB
                        + recordDate);
    }

    public Text getMapperKeyOutput(String keyword) {
        return new Text(keyword + Constants.TAB
                + fileName + Constants.TAB
                + GetProvinceNameUtil.getProvinceName(values[0]));
    }
}