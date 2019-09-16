package cn.spark.java.example.core;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.Arrays;
import java.util.List;

/**
 * @ClassName: SparkConfigLocalApp
 * @Description:
 * @author: zhangyi
 **/
public class SparkConfigLocalApp {

    public static SparkConf conf;

    public static JavaSparkContext sc;

    public static JavaRDD<String> linesRDD;

    public static List<Tuple2<String, Integer>> tuple2List;

    static {
        conf = new SparkConf().setAppName("sparkJavaExample").setMaster("local");
        sc = new JavaSparkContext(conf);
        linesRDD = sc.textFile("/Users/zhangyi/Desktop/spark.txt");
    }

    static {
        //数据模拟
        tuple2List = Arrays.asList(new Tuple2<>("a", 1),
                new Tuple2<>("b", 2),
                new Tuple2<>("a", 3),
                new Tuple2<>("b", 4));
    }
}
