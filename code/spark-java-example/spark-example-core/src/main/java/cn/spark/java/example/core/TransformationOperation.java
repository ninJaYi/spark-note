package cn.spark.java.example.core;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.junit.Test;
import scala.Tuple2;

import java.util.Arrays;
import java.util.List;

import static cn.spark.java.example.core.SparkConfigLocalApp.*;

/**
 * @ClassName: TransformationOperation
 * @Description:
 * @author: zhangyi
 **/
public class TransformationOperation {


    /**
     * map算子操作
     * 对原始RDD进行计算处理操作，产生新的RDD
     */
    @Test
    public void map() {

        List<Integer> numbers = Arrays.asList(1, 2, 3, 4, 5);

        //并行化集合，初始化RDD
        JavaRDD<Integer> numberRDD = sc.parallelize(numbers);

        JavaRDD<Integer> multipleNumberRDD = numberRDD.map(v -> v * 2);

        multipleNumberRDD.foreach(v -> System.out.println(v));
    }

    /**
     * filter
     * 过滤RDD元素
     * <p>
     * filter中接收function 中的函数返回值为boolean
     * <p>
     * 保留过滤后的RDD元素返回true,反之false
     */
    @Test
    public void filter() {
        List<Integer> numbers = Arrays.asList(1, 2, 3, 4, 5);

        JavaRDD<Integer> numberRDD = sc.parallelize(numbers);

        JavaRDD<Integer> evenNumnberRDD = numberRDD.filter(v -> v % 2 == 0);

        evenNumnberRDD.foreach(v -> System.out.println(v));
    }


    /**
     * flatMap
     * 拆分RDD元素
     */
    @Test
    public void flatMap() {

        List<String> lineArry = Arrays.asList("hello word", "hello spark", "hello java");

        JavaRDD<String> lineArrayRDD = sc.parallelize(lineArry);

        JavaRDD<String> stringJavaRDD = lineArrayRDD.flatMap(line -> Arrays.asList(line.split(" ")).iterator());

        stringJavaRDD.foreach(v -> System.out.println(v));
    }


    /**
     * groupByKey
     * 对tuple2元素的key元素进行分组
     */
    @Test
    public void groupByKey() {

        JavaPairRDD<String, Integer> tuple2RDD =sc.parallelizePairs(tuple2List);

        JavaPairRDD<String, Iterable<Integer>> newRdd = tuple2RDD.groupByKey();

        newRdd.foreach(v -> System.out.println(v));
    }

    /**
     * reduceByKey
     * 对tuple2元素的key进行统计
     * 对每个Key 的value 进行聚合操作，得出每个key 对应的一个聚合后的value
     * 组合成一个Tuple2 最为新的RDD元素
     */
    @Test
    public void reduceByKey() {
        JavaPairRDD<String, Integer> tuple2RDD = sc.parallelizePairs(tuple2List);

        JavaPairRDD<String, Integer> newRDD = tuple2RDD.reduceByKey((v1, v2) -> v1 + v2);

        newRDD.foreach(v -> System.out.println(v));
    }

    /**
     * sortByKey
     * 对tuple2元素的key进行排序
     */
    @Test
    public void sortBykey() {

        JavaPairRDD<String, Integer> tuple2RDD = sc.parallelizePairs(tuple2List);

        JavaPairRDD<String, Integer> newRDD = tuple2RDD.sortByKey();

        newRDD.foreach(v -> System.out.println(v));
    }

    /**
     * join 和 cogroup
     */
    @Test
    public void joinAndCogroup() {
        // 模拟集合
        List<Tuple2<Integer, String>> studentList = Arrays.asList(
                new Tuple2<Integer, String>(1, "leo"),
                new Tuple2<Integer, String>(2, "jack"),
                new Tuple2<Integer, String>(3, "tom"));

        List<Tuple2<Integer, Integer>> scoreList = Arrays.asList(
                new Tuple2<Integer, Integer>(1, 100),
                new Tuple2<Integer, Integer>(2, 90),
                new Tuple2<Integer, Integer>(3, 60));


        // 并行化两个RDD
        JavaPairRDD<Integer, String> students = sc.parallelizePairs(studentList);
        JavaPairRDD<Integer, Integer> scores = sc.parallelizePairs(scoreList);

        //join
        JavaPairRDD<Integer, Tuple2<String, Integer>> joinRDD = students.join(scores);

        //cogroup
        JavaPairRDD<Integer, Tuple2<Iterable<String>, Iterable<Integer>>> cogroupRDD = students.cogroup(scores);

        joinRDD.foreach(v -> System.out.println(v));

        cogroupRDD.foreach(v -> System.out.println(v));
    }


}
