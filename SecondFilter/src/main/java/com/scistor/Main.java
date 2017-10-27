package com.scistor;

import com.google.common.hash.BloomFilter;
import com.google.common.hash.Funnels;
import com.scistor.Utils.RedisUtil;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;

import java.util.*;

public class Main
{
    private static final Logger logger = Logger.getLogger(Main.class);

    public static void main( String[] args ) {
        if (args.length < 2) {
            logger.error("Please specify the inputFilePath and outputFilePath when running this program!");
            return;
        }
        String inputFilePath = args[0];
        String outputFilePath = args[1];
        SparkConf sparkConf = new SparkConf().setAppName("SecondFilter").setMaster("spark://172.16.18.234:7077");
        JavaSparkContext javaSparkContext = new JavaSparkContext(sparkConf);
        JavaRDD<String> stringJavaRDD = javaSparkContext.textFile(inputFilePath);
        final BloomFilter<CharSequence> bloomFilter = generateBloomFilter();
        JavaRDD<String> filterRDD = stringJavaRDD.filter(new Function<String, Boolean>() {
            public Boolean call(String line) throws Exception {
                logger.debug(String.format("The current line is: [%s]", line));
                String host = line.split("\\|\\|")[0];
                boolean mightContain = bloomFilter.mightContain(host);
                if (mightContain) {
                    return false;
                } else {
                    return true;
                }
            }
        });
        logger.info(String.format("Writing filtered data to the output path: [%s]", outputFilePath));
        filterRDD.saveAsTextFile(outputFilePath);
    }

    private static BloomFilter<CharSequence> generateBloomFilter() {
        logger.info(String.format("Start to generate the BloomFilter!"));
        BloomFilter<CharSequence> filter = BloomFilter.create(Funnels.stringFunnel(), 100, 0.0001F);
        TreeSet<String> keySets = RedisUtil.getRedisKeys();
        Iterator<String> it = keySets.iterator();
        while (it.hasNext()) {
            filter.put(it.next());
        }
        logger.info(String.format("The BloomFilter has been generated!"));
        return filter;
    }

}
