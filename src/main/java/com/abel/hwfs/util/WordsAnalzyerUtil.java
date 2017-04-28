package com.abel.hwfs.util;

import java.io.IOException;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.List;

import org.apache.log4j.Logger;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.wltea.analyzer.lucene.IKAnalyzer;

/**
 * 使用IKAnalyzer进行分词
 *
 */
public class WordsAnalzyerUtil {

    private static Logger log = Logger.getLogger(WordsAnalzyerUtil.class);

    public static List<String> WordsAnalzyer(String searchWords) {
        // 构建IK分词器，使用smart分词模式
        Analyzer analyzer = new IKAnalyzer(true);

        // 获取Lucene的TokenStream对象
        TokenStream ts = null;
        List<String> wordsList = null;
        try {
            ts = analyzer.tokenStream("myfield", new StringReader(searchWords));
            // 获取词元位置属性
            // OffsetAttribute offset = ts.addAttribute(OffsetAttribute.class);
            // //获取词元文本属性
            CharTermAttribute term = ts.addAttribute(CharTermAttribute.class);
            // //获取词元文本属性
            // TypeAttribute type = ts.addAttribute(TypeAttribute.class);

            // 重置TokenStream（重置StringReader）
            ts.reset();

            // 迭代获取分词结果
            wordsList = new ArrayList<String>();
            while (ts.incrementToken()) {
                // System.out.println(offset.startOffset() + " - " +
                // offset.endOffset() + " : " + term.toString() + " | " +
                // type.type());
                wordsList.add(term.toString());
            }
            // 关闭TokenStream（关闭StringReader）
            ts.end(); // Perform end-of-stream operations, e.g. set the final
                      // offset.

        } catch (IOException e) {
            log.error(e.getMessage());
            e.printStackTrace();
        } finally {
            // 释放TokenStream的所有资源
            if (ts != null) {
                try {
                    ts.close();
                } catch (IOException e) {
                    log.error(e.getMessage());
                    e.printStackTrace();
                }
            }
        }

        return wordsList;
    }

}
