package com.abel.hwfs.util;

import org.apache.hadoop.io.Text;

public class GetProvinceNameUtil {

    public static String getProvinceName(String record) {
        String [] provinceNames = {"北京", "天津", "上海", "重庆", "河北", "山西",
                "辽宁", "吉林", "黑龙江", "江苏", "浙江", "安徽", "福建", "江西",
                "山东", "河南", "湖北", "湖南", "广东", "海南", "四川", "贵州",
                "云南", "陕西", "甘肃", "青海", "台湾", "内蒙古", "广西",
                "西藏", "宁夏", "新疆", "香港", "澳门"};

        String searchUserId = new RecordSplitUtil(new Text(record)).getSearchUserId();
        int provinceId;
        try {
            provinceId = Integer.parseInt(searchUserId.substring(0, 5)) % provinceNames.length;
        } catch (Exception e) {
            provinceId = (int)(Math.random() * provinceNames.length);
        }
        return provinceNames[provinceId];
    }
}
