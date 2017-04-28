package com.abel.hwfs.main;

import java.io.IOException;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.db.DBConfiguration;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.log4j.Logger;
import org.mortbay.log.Log;

import com.abel.hwfs.common.Constants;
import com.abel.hwfs.custom.output.SetSizeDBOutputFormat;
import com.abel.hwfs.custom.output.WriteDataToDB.WordDetailTableWritable;
import com.abel.hwfs.util.GetProvinceNameUtil;
import com.abel.hwfs.util.MapperOutUtil;
import com.abel.hwfs.util.PropertiesUtil;
import com.abel.hwfs.util.RecordSplitUtil;
import com.abel.hwfs.util.WordsAnalzyerUtil;

public class HotWordsCount {
    public static class TokenizerMapper extends Mapper<Object, Text, Text, Text> {

        private static Logger log = Logger.getLogger(TokenizerMapper.class);

        @Override
        public void map(Object key, Text value, Context context)
                throws IOException, InterruptedException {

            String fileName = ((FileSplit) context.getInputSplit()).getPath().getName();
            MapperOutUtil mapperOut = new MapperOutUtil(value, fileName);
            List<String> keywordsList = WordsAnalzyerUtil.WordsAnalzyer(mapperOut.getContext());
            for (String keyword : keywordsList) {
                context.write(mapperOut.getMapperKeyOutput(keyword), mapperOut.getMapperValueOutput(keyword));
            }
        }
    }

    public static class OutputObjectSumReducer extends Reducer<Text, Text, WordDetailTableWritable, WordDetailTableWritable> {

        private static Logger log = Logger.getLogger(TokenizerMapper.class);

        private final IntWritable result = new IntWritable();

        @Override
        public void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {

            // A word total count(Include repeat).
            int sum = 0;
            Text keyOutput = new Text();
            // Filter repeat record by same user.
            Set<String> recordsSet = new HashSet<String>();
            for (Text val : values) {
                sum++;
                recordsSet.add(val.toString());
            }

            // All user search repeat count.
            int repeatSearchCountOneDay = sum - recordsSet.size();
            /**
             *  1. Random a num to mock a province message.
             *  2. Filter repeat record by same province.
             *  3. Use province filed replace
             */
            Set<String> filterProvinceSet = new HashSet<String>();
            result.set(recordsSet.size());
            for (String record : recordsSet) {
                record = GetProvinceNameUtil.getProvinceName(new RecordSplitUtil(new Text(record)).getSearchUserId()) + Constants.TAB
                       + record.substring(record.indexOf(Constants.TAB) + 1) + Constants.TAB
                       + result + Constants.TAB
                       + repeatSearchCountOneDay;
                filterProvinceSet.add(record);
            }

            // Write record to DB
            for (String record : filterProvinceSet) {
                keyOutput.set(record);
                context.write(new WordDetailTableWritable(keyOutput),null);
            }
        }

    }

    public static void main(String[] args) throws Exception {

        long beginTime = System.currentTimeMillis();

        Configuration conf = new Configuration();

        DBConfiguration.configureDB(conf,
                                    PropertiesUtil.getProperty("JDBC_DRIVER"),
                                    PropertiesUtil.getProperty("JDBC_URL"),
                                    PropertiesUtil.getProperty("JDBC_USER"),
                                    PropertiesUtil.getProperty("JDBC_PASSWORD"));

        Job job = new Job(conf, "hot words count");
//        File jarFile = EJob.createTempJar("bin");
//        System.out.println("jarFile == " + jarFile);
//        ((JobConf) job.getConfiguration()).setJar(jarFile.toString());

        job.setJarByClass(HotWordsCount.class);
        job.setMapperClass(TokenizerMapper.class);
//        job.setCombinerClass(IntSumReducer.class);
        job.setReducerClass(OutputObjectSumReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        job.setOutputKeyClass(WordDetailTableWritable.class);
        job.setOutputValueClass(WordDetailTableWritable.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(SetSizeDBOutputFormat.class);

        FileInputFormat.addInputPath(job, new Path(PropertiesUtil.getProperty("INPUT_PATH")));

        SetSizeDBOutputFormat.setOutput(job,
                                 PropertiesUtil.getProperty("TABLE_NAME"),
                                 PropertiesUtil.getProperty("FIELD_ONE"),
                                 PropertiesUtil.getProperty("FIELD_TWO"),
                                 PropertiesUtil.getProperty("FIELD_THREE"),
                                 PropertiesUtil.getProperty("FIELD_FOUR"),
                                 PropertiesUtil.getProperty("FIELD_FIVR"),
                                 PropertiesUtil.getProperty("FIELD_SIX"));

        boolean isExit = job.waitForCompletion(true);

        long endTime=System.currentTimeMillis();
        Log.info("The MapReduce run is " + (isExit ? "successful! " : "failed! ")
                 + "Use Time : "+ (endTime - beginTime) + "(about" + (int)((double)(endTime - beginTime) / 60000) + "Min)");

        System.exit(isExit ? 0 : 1);
    }
}
