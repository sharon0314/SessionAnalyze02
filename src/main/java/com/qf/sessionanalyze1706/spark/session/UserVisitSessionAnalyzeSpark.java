package com.qf.sessionanalyze1706.spark.session;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.qf.sessionanalyze1706.constant.Constants;
import com.qf.sessionanalyze1706.dao.*;
import com.qf.sessionanalyze1706.dao.factory.DAOFactory;
import com.qf.sessionanalyze1706.domain.*;
import com.qf.sessionanalyze1706.util.*;
import org.apache.spark.Accumulator;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.*;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.storage.StorageLevel;
import parquet.it.unimi.dsi.fastutil.ints.IntArrayList;
import parquet.it.unimi.dsi.fastutil.ints.IntList;
import scala.Tuple2;

import java.util.*;

/**
 * 获取用户访问session数据进行分析
 * 1.时间范围：起始-结束
 *   年龄范围
 *   性别
 *   职业
 *   城市
 *   用户搜索关键字
 *   点击品类
 *   点击商品
 *  2.spark作业是如何接收使用者创建的任务信息
 *  1）shell脚本通知调用spark-submit脚本
 *  2）从mysql的task表中根据指定的taskId获取任务信息
 *  3.spark作业开始数据分析
 */
public class UserVisitSessionAnalyzeSpark {
    public static void main(String[] args) {
        //模板代码，创建配置信息类
        SparkConf conf= new SparkConf()
                .setAppName("UserVisitSessionAnalyzeSpark");
        SparkUtils.setMaster(conf);
        //创建集群入口类
        JavaSparkContext sc= new JavaSparkContext(conf);
        //sparksql的上下文对象
        SQLContext sqlContext=SparkUtils.getSQLContext(sc.sc());

        /**
         * 设置检查点:
         * shuffle后的数据比较重要，
         * 保证数据安全性所以设置检查点再hdfs中，
         * 而且如若数据很多重新shuffle很麻烦
         */
 //       sc.checkpointFile("hdfs://hadoop01:9000/data/");
        //生成模拟数据
        SparkUtils.mockData(sc,sqlContext);
        //创建获取任务信息的实例
       ITaskDAO taskDAO = DAOFactory.getTaskDAO();
        //获取指定的任务，需要拿到taskId
        Long taskId = ParamUtils.getTaskIdFromArgs(args, Constants.SPARK_LOCAL_TASKID_SESSION);
        //根据taskId获取任务信息
        Task task = taskDAO.findById(taskId);

        if (task == null){
            System.out.println(new Date() + "亲，你的taskId我不能获取到信息");
        }

        //根据task去task_param字段去获取对应的任务信息
        //task_param字段里面存储的就是使用者提供的查询条件
        JSONObject taskParam = JSON.parseObject(task.getTaskParam());

        //开始查询指定日期的行为数据（点击、收索、下单、支付）
        //首先要从user_visit_action这张hive表中查询出指定日期范围的行为数据
       JavaRDD<Row> actionRDD = SparkUtils.getActionRDDByDateRange(sqlContext,taskParam);
        /**
         * 2018-08-14
         *
         *作业：
         * 1.session分析中的session概念描述
         *用户session分析介绍：用户浏览电商网站会产生很多浏览行为，比如：
         * 进入首页，点击某个品类，点击某个商品，下单，支付，推出网站
         * 浏览行为：用户的每一次操作都i是可以理解为一个action:比如点击，搜索，下单，支付，每一个动作对应一条数据用户访问session指的是，从用户第一次进入首页到session开始，在一定的时间范围内做的一系列的操作，直到最后操作完成，这个过程可能做了几十次或者上百次的操作，只要离开或者关闭或者长时间不进行操作，该session结束以上用户在网站上访问的整个行为过程，称为session，所以session就是电商网站中最基本的数据，属于面相用户端的分析
         * 2.项目技术架构的理解
         * 见D:\hadoop_learning\spark_project\day01
         * 3.项目需求的理解
         * 4.今天代码的理解
         * 预习：
         * 1.用户访问session进行聚合统计需求的实现思路
         * spark作业时分布式的，所以每个sparktask在执行任务的时候，需要对一个全局变量进行累加操作，通过自定义Accumulator技术，
         * 来实现复杂的分布式计算
         * 2.按时间比例随机抽取session需求的实现思路
         * 猜想：
         * 按照时间比例进行分组groupByKey，统计每组的session数量countByKey
         * 用到的算子：
         * countByKey、groupByKey、mapToPair
         */
        /**
         * 2018-08-15
         *
         */
        //生成session力度的基础数据，得到的数据格式：<sessionId,actionRDD>
        JavaPairRDD<String,Row> sessionId2ActionRDD =getSessionId2ActionRDD(actionRDD);

        //经常用到的数据要缓存起来，便于以后快速的获取该数据
        //基础数据很重要先做一次cache进行缓存
        sessionId2ActionRDD = sessionId2ActionRDD.cache();

        /**
         * 对行为数据进行聚合
         * 1.将行为数据按照session进行分组
         * 2.行为数据RDD需要和用户信息获取到，此时用到join，得到session粒度的明细数据
         * 明细数据包含了session对应的用户基本信息：
         * 生成的格式为：<sessionId,(sessionId,searchKeywords,clickCategoryIds,age,professonal)></>
         */

// 对行为数据进行聚合
        // 1、将行为数据按照sessionId进行分组
        // 2、行为数据RDD需要把用户信息获取到，此时需要用到join，这样得到session粒度的明细数据
        //      明细数据包含了session对应的用户基本信息：
        // 生成的格式为：<sessionId, (sessionId, searchKeywords, clickCategoryIds, age, professional, city, sex)>
        JavaPairRDD<String, String> sessionId2AggrInfoRDD = aggrgateBySession(sc, sqlContext, sessionId2ActionRDD);

        // 实现Accumulator累加器对数据字段的值进行累加
        Accumulator<String> sessionAggrStatAccumulator = sc.accumulator("", new SessionAggrStatAccumulator());


        // 以session粒度的数据进行聚合，按照使用者指定的筛选条件进行过滤
        JavaPairRDD<String, String> filteredSessionId2AggrInfoRDD = filteredSessionAndAggrStat(sessionId2AggrInfoRDD, taskParam, sessionAggrStatAccumulator);

        // 缓存过滤后的数据
        filteredSessionId2AggrInfoRDD = filteredSessionId2AggrInfoRDD.persist(StorageLevel.MEMORY_AND_DISK());

        // 生成一个公共的RDD， 通过筛选条件过滤出来的session得到访问明细
        JavaPairRDD<String, Row> sessionId2DetailRDD = getSessionId2DetailRDD(filteredSessionId2AggrInfoRDD, sessionId2ActionRDD);

        // 缓存
        sessionId2DetailRDD = sessionId2DetailRDD.cache();

        // 如果将上一个聚合的统计结果写入数据库，
        // 就必须给一个action算子进行触发后才能真正执行任务，从Accumulator中获取数据
        System.out.println(sessionId2DetailRDD.count());

        // 计算出各个范围的session占比，并写入数据库
        calculateAndPersistAggrStat(sessionAggrStatAccumulator.value(), task.getTaskid());

        /**
         * 按照时间比例随机抽取session
         * 1、首先计算出每个小时的session数量
         * 2、计算出每个小时的session数据量在一天的比例
         *      比如：要取出100条session数据
         *      表达式：当前小时抽取的session数量 = （每小时session的数据量 / session的总量）* 100
         * 3、按照比例进行随机抽取session
         */
        randomExtranctSession(sc, task.getTaskid(), filteredSessionId2AggrInfoRDD, sessionId2DetailRDD);

        /**
         * 获取点击下单支付的品类次数排名top10
         * 1.获取通过筛选条件的session访问过的所有品类
         * 2.计算出session访问过的所有的品类的点击，下单支付次数，用join
         * 3.自定义排序类
         * 4.需要将品类的点击下单支付次数封装到自定义排序key中
         * 5.使用sortByKey进行二次排序
         * 6.获取排序后的前十个品类take()
         * 7.将top10热门品类以及每个品类的点击下单支付次数写入数据库
         */
        List<Tuple2<CategorySortKey,String>> top10CategoryList = getTop10Category(task.getTaskid(),sessionId2DetailRDD);

        /**
         * 获取top10活跃session
         * 1.获取到符合帅选条件的session明细数据top10CategoryList
         * 2.按照sessiosn粒度的数据进行聚合，获取到session对应的每个品类的点击次数
         * 3.按照品类id，分组取top10，并且获取top10活跃session
         * 4.结果的存储
         */
        getTop10Session(sc,taskId,top10CategoryList,sessionId2DetailRDD);

        sc.stop();
    }
    private static void getTop10Session(JavaSparkContext sc, final Long taskId, List<Tuple2<CategorySortKey, String>> top10CategoryList, JavaPairRDD<String, Row> sessionId2DetailRDD) {
        /*第一步:将top10热门品类的id转化为RDD*/
        // 为了方便操作,将top10CategoryList中的categoryId放到一个List中
        // 格式为:<categoryId,categoryId>
        List<Tuple2<Long, Long>> top10CategoryIdList = new ArrayList<>();
        for (Tuple2<CategorySortKey, String> category : top10CategoryList) {
            long categoryId = Long.valueOf(StringUtils.getFieldFromConcatString(category._2, "\\|", Constants.FIELD_CATEGORY_ID));
            top10CategoryIdList.add(new Tuple2<>(categoryId, categoryId));
        }
        // 将top10CategoryIdList转换成RDD
        JavaPairRDD<Long, Long> top10CategoryIdRDD = sc.parallelizePairs(top10CategoryIdList);
        /*第二步:计算top10品类被各个Session点击的次数*/
        // 以sessionId进行分组
        JavaPairRDD<String, Iterable<Row>> groupedSessionId2DetailRDD = sessionId2DetailRDD.groupByKey();
        // 把品类id对应的Session和Count数据生成格式为:<categoryId,"sessionId,count">
        JavaPairRDD<Long, String> categoryId2SessionCountRDD = groupedSessionId2DetailRDD.flatMapToPair(new PairFlatMapFunction<Tuple2<String, Iterable<Row>>, Long, String>() {
            @Override
            public Iterable<Tuple2<Long, String>> call(Tuple2<String, Iterable<Row>> stringIterableTuple2) throws Exception {
                String sessionId = stringIterableTuple2._1;
                Iterator<Row> iterator = stringIterableTuple2._2.iterator();
                // 用于存储品类对应的点击次数:<key=categoryId,value=次数>
                Map<Long, Long> categoryCountMap = new HashMap<>();
                // 计算出session对应的每个品类的点击次数
                while (iterator.hasNext()) {
                    Row row = iterator.next();
                    if (row.get(6) != null) {
                        long categoryId = row.getLong(6);
                        Long count = categoryCountMap.get(categoryId);
                        if (count == null)
                            count = 0L;
                        count++;
                        categoryCountMap.put(categoryId, count);
                    }
                }
                // 返回结果到一个List,格式为:<categoryId,"sessionId,count">
                List<Tuple2<Long, String>> list = new ArrayList<>();
                for (Map.Entry<Long, Long> categoryCountEntry : categoryCountMap.entrySet()) {
                    long categoryId = categoryCountEntry.getKey();
                    long count = categoryCountEntry.getValue();
                    String value = sessionId + "," + count;
                    list.add(new Tuple2<>(categoryId, value));
                }
                return list;
            }
        });
        // 获取到top10热门品类被各个session点击的次数
        JavaPairRDD<Long, String> top10CategorySessionCountRDD = top10CategoryIdRDD.join(categoryId2SessionCountRDD).mapToPair(new PairFunction<Tuple2<Long, Tuple2<Long, String>>, Long, String>() {
            @Override
            public Tuple2<Long, String> call(Tuple2<Long, Tuple2<Long, String>> longTuple2Tuple2) throws Exception {
                return new Tuple2<>(longTuple2Tuple2._1, longTuple2Tuple2._2._2);
            }
        });
        /*第三步:分组取topN算法,实现获取每个品类的top10活跃用户*/
        // 以categoryId进行分组
        JavaPairRDD<Long, Iterable<String>> groupedTop10CategorySessionCountRDD = top10CategorySessionCountRDD.groupByKey();
        // 取各个品类中的前10个session<sessionId,sessionId>

        //白玉锴 2018.8.20 10:29:57
        JavaPairRDD<String, String> top10SessionRDD = groupedTop10CategorySessionCountRDD.flatMapToPair(new PairFlatMapFunction<Tuple2<Long, Iterable<String>>, String, String>() {
            @Override
            public Iterable<Tuple2<String, String>> call(Tuple2<Long, Iterable<String>> longIterableTuple2) throws Exception {
                long categoryId = longIterableTuple2._1;
                Iterator<String> iterator = longIterableTuple2._2.iterator();
                // 声明一个数组,用来存储topN的排序数组(存储品类中的前10个session)
                String[] top10Sessions = new String[10];
                while (iterator.hasNext()) {
                    // "session,count"
                    String sessionCount = iterator.next();
                    long count = Long.valueOf(sessionCount.split(",")[1]);
                    // 遍历排序数组(topN算法)
                    for (int i = 0; i < top10Sessions.length; i++) {
                        // 判断,如果当前索引下的数据为空,就直接将sessionCount赋值给当前的i位数据
                        if (top10Sessions[i] == null) {
                            top10Sessions[i] = sessionCount;
                            break;
                        } else {
                            long _count = Long.valueOf(top10Sessions[i].split(",")[1]);
                            // 判断,如果sessionCount比i位的sessionCount大
                            // 从排序数组最后一位开始,到i位,所有的数据往后挪一位
                            if (count > _count) {
                                for (int j = 9; j > i; j--) {
                                    top10Sessions[j] = top10Sessions[j - 1];
                                }
                                // 将sessionCount赋值为top10Sessions的i位数据
                                top10Sessions[i] = sessionCount;
                                break;
                            }
                        }
                    }
                }
                // 用于存储top10Session里的sessionId,格式为:<sessionId,sessionId>
                List<Tuple2<String, String>> list = new ArrayList<>();
                // 将数据写入数据库
                ITop10SessionDAO top10SessionDAO = DAOFactory.getTop10SessionDAO();
               // top10SessionDAO.truncate();
                for (String sessionCount : top10Sessions) {
                    if (sessionCount != null) {
                        String sessionId = sessionCount.split(",")[0];
                        long count = Long.valueOf(sessionCount.split(",")[1]);
                        Top10Session top10Session = new Top10Session();
                        top10Session.setTaskid(taskId);
                        top10Session.setCategoryid(categoryId);
                        top10Session.setSessionid(sessionId);
                        top10Session.setClickCount(count);
                        top10SessionDAO.insert(top10Session);
                        list.add(new Tuple2<>(sessionId, sessionId));
                    }
                }
                return list;
            }
        });
        /**
         * 第四步:获取top10活跃Session的明细数据并持久化
         */
        JavaPairRDD<String,Tuple2<String,Row>> sessionDetailRDD =
                top10SessionRDD.join(sessionId2DetailRDD);

        sessionDetailRDD.foreach(new VoidFunction<Tuple2<String, Tuple2<String, Row>>>() {
            @Override
            public void call(Tuple2<String, Tuple2<String, Row>> stringTuple2Tuple2) throws Exception {
                // 获取Session对应的明细数据
                Row row = stringTuple2Tuple2._2._2;
                SessionDetail sessionDetail = new SessionDetail();
                sessionDetail.setTaskid(taskId);
                sessionDetail.setUserid(row.getLong(1));
                sessionDetail.setSessionid(row.getString(2));
                sessionDetail.setPageid(row.getLong(3));
                sessionDetail.setActionTime(row.getString(4));
                sessionDetail.setSearchKeyword(row.getString(5));
                sessionDetail.setClickCategoryId(row.getLong(6));
                sessionDetail.setClickProductId(row.getLong(7));
                sessionDetail.setOrderCategoryIds(row.getString(8));
                sessionDetail.setOrderProductIds(row.getString(9));
                sessionDetail.setPayCategoryIds(row.getString(10));
                sessionDetail.setPayProductIds(row.getString(11));
                ISessionDetailDAO sessionDetailDAO = DAOFactory.getSessionDetailDAO();
                sessionDetailDAO.insert(sessionDetail);
            }
        });

    }

    private static List<Tuple2<CategorySortKey,String>> getTop10Category(
            long taskid,
            JavaPairRDD<String, Row> sessionId2DetailRDD) {
        /**
         * 第一步：获取符合条件的session访问的所有品类
         */
        //获取session访问呢的所有品类的id（访问过指的是点击过、下单过、支付过）
        JavaPairRDD<Long,Long> categoryIdRDD = sessionId2DetailRDD.flatMapToPair(
                new PairFlatMapFunction<Tuple2<String, Row>, Long, Long>() {
                    @Override
                    public Iterable<Tuple2<Long, Long>> call(Tuple2<String, Row> stringRowTuple2) throws Exception {
                            Row row = stringRowTuple2._2;
                        //用于存储点击、下单、支付品类信息
                        List<Tuple2<Long,Long>> list = new ArrayList<Tuple2<Long,Long>>();
                        //添加点击品类信息
                        Long clickCategoryId = row.getLong(6);
                        if (clickCategoryId != null){
                            list.add(new Tuple2<Long,Long>(clickCategoryId,clickCategoryId));
                        }
                        //添加下单信息
                        String orderCategoryIds =row.getString(8);
                        if (orderCategoryIds != null){
                            String[] orderCategoryIdsSplited = orderCategoryIds.split(",");
                            for (String payCategoryId:orderCategoryIdsSplited){
                                Long longPayCategoryId = Long.valueOf(payCategoryId);
                                list.add(new Tuple2<Long,Long>(longPayCategoryId,longPayCategoryId));
                            }
                        }

                        return list;
                    }
                });
        /**
         * session访问呢过的所有品类中，可能有重复的categoryId，需要去重
         */
        categoryIdRDD = categoryIdRDD.distinct();

        /**
         * 第二步：计算各品类点击，下单，支付次数
         */
        //计算各品类的点击次数
        JavaPairRDD<Long,Long> clickCategoryId2CountRDD =
                getClickCategoryId2CountRDD(sessionId2DetailRDD);
        //计算各品类的下单次数
        JavaPairRDD<Long,Long> orderCategoryId2CountRDD =
                getOrderCategoryId2CountRDD(sessionId2DetailRDD);
        //计算各皮内的支付次数
        JavaPairRDD<Long,Long> payCategoryId2CountRDD =
                getPayCategoryId2CountRDD(sessionId2DetailRDD);
        /**
         * 第三步：join各品类和他的点击、下单、支付次数
         * categoryIdRDD数据里面，包含了所有符合条件的过滤掉重复品类的session
         * 在第二步中分别计算了点击下单支付次数，可能不是包含所有品类的
         * 比如：有的品类只是点击过，但没有下单，类似的这种情况有很多
         * 所以，在这里如果要做join，就不能用join，需要用leftOuterJoin
         */
        JavaPairRDD<Long, String> categoryId2CountRDD = joinCategoryAndDetail(
                categoryIdRDD,
                clickCategoryId2CountRDD,
                orderCategoryId2CountRDD,
                payCategoryId2CountRDD
        );

        /**
         * 第四步：实现自定义排序类
         */

        /**
         * 第五步：将数据映射为：<CategorySortKey, countInfo>, 再进行二次排序
         */
        JavaPairRDD<CategorySortKey, String> sortKeyCountRDD = categoryId2CountRDD.mapToPair(
                new PairFunction<Tuple2<Long, String>, CategorySortKey, String>() {
                    @Override
                    public Tuple2<CategorySortKey, String> call(Tuple2<Long, String> tup) throws Exception {
                        String countInfo = tup._2;

                        long clickCount = Long.valueOf(StringUtils.getFieldFromConcatString(countInfo, "\\|", Constants.FIELD_CLICK_COUNT));
                        long orderCount = Long.valueOf(StringUtils.getFieldFromConcatString(countInfo, "\\|", Constants.FIELD_ORDER_COUNT));
                        long payCount = Long.valueOf(StringUtils.getFieldFromConcatString(countInfo, "\\|", Constants.FIELD_PAY_COUNT));

                        // 创建自定义排序实例
                        CategorySortKey sortKey = new CategorySortKey(clickCount, orderCount, payCount);

                        return new Tuple2<CategorySortKey, String>(sortKey, countInfo);
                    }
                });

        // 进行降序排序
        JavaPairRDD<CategorySortKey, String> sortedCategoryCountRDD =
                sortKeyCountRDD.sortByKey(false);


        /**
         * 第六步: 取出top10热门品类并持久化到数据库
         */
        List<Tuple2<CategorySortKey, String>> top10CategoryList = sortedCategoryCountRDD.take(10);

        ITop10CategoryDAO top10CategoryDAO = DAOFactory.getTop10CategoryDAO();
        for (Tuple2<CategorySortKey, String> tuple : top10CategoryList) {
            String countInfo = tuple._2;
            long categoryId = Long.valueOf(StringUtils.getFieldFromConcatString(countInfo, "\\|", Constants.FIELD_CATEGORY_ID));
            long clickCount = Long.valueOf(StringUtils.getFieldFromConcatString(countInfo, "\\|", Constants.FIELD_CLICK_COUNT));
            long orderCount = Long.valueOf(StringUtils.getFieldFromConcatString(countInfo, "\\|", Constants.FIELD_ORDER_COUNT));
            long payCount = Long.valueOf(StringUtils.getFieldFromConcatString(countInfo, "\\|", Constants.FIELD_PAY_COUNT));

            Top10Category top10Category = new Top10Category();
            top10Category.setTaskid(taskid);
            top10Category.setCategoryid(categoryId);
            top10Category.setClickCount(clickCount);
            top10Category.setOrderCount(orderCount);
            top10Category.setPayCount(payCount);

            top10CategoryDAO.insert(top10Category);
        }

        return top10CategoryList;
    }

    /**
     * 连接品类RDD和数据RDD
     * @param categoryIdRDD
     * @param clickCategoryId2CountRDD
     * @param orderCategoryId2CountRDD
     * @param payCategoryId2CountRDD
     * @return
     */
    private static JavaPairRDD<Long, String> joinCategoryAndDetail(
            JavaPairRDD<Long, Long> categoryIdRDD,
            JavaPairRDD<Long, Long> clickCategoryId2CountRDD,
            JavaPairRDD<Long, Long> orderCategoryId2CountRDD,
            JavaPairRDD<Long, Long> payCategoryId2CountRDD) {
        // 注意：如果用leftOuterJoin，就可能出现右边RDD中join过来的值为空的情况
        // 所有tuple中的第二个值用Optional<Long>类型，代表可能有值，也可能没有值
        JavaPairRDD<Long, Tuple2<Long, com.google.common.base.Optional<Long>>> tmpJoinRDD =
                categoryIdRDD.leftOuterJoin(clickCategoryId2CountRDD);

        // 把数据生成格式为: (categoryId, "categoryId=品类|clickCount=点击次数")
        JavaPairRDD<Long, String> tmpMapRDD = tmpJoinRDD.mapToPair(
                new PairFunction<Tuple2<Long, Tuple2<Long, com.google.common.base.Optional<Long>>>, Long, String>() {
                    @Override
                    public Tuple2<Long, String> call(Tuple2<Long, Tuple2<Long, com.google.common.base.Optional<Long>>> tup) throws Exception {

                        long categoryId = tup._1;
                        com.google.common.base.Optional<Long> optional = tup._2._2;

                        long clickCount = 0L;

                        if (optional.isPresent()) {
                            clickCount = optional.get();
                        }

                        String value = Constants.FIELD_CATEGORY_ID + "=" + categoryId + "|" +
                                Constants.FIELD_CLICK_COUNT + "=" + clickCount;

                        return new Tuple2<Long, String>(categoryId, value);
                    }
                });

        // 再次与下单次数进行leftOuterJoin
        tmpMapRDD = tmpMapRDD.leftOuterJoin(orderCategoryId2CountRDD).mapToPair(
                new PairFunction<Tuple2<Long, Tuple2<String, com.google.common.base.Optional<Long>>>, Long, String>() {
                    @Override
                    public Tuple2<Long, String> call(Tuple2<Long, Tuple2<String, com.google.common.base.Optional<Long>>> tup) throws Exception {
                        long categoryId = tup._1;
                        String value = tup._2._1;
                        com.google.common.base.Optional<Long> optional = tup._2._2;

                        long orderCount = 0L;

                        if (optional.isPresent()) {
                            orderCount = optional.get();
                        }

                        value = value + "|" + Constants.FIELD_ORDER_COUNT + "=" + orderCount;

                        return new Tuple2<Long, String>(categoryId, value);
                    }
                });

        // 再与支付次数进行leftOuterJoin
        tmpMapRDD = tmpMapRDD.leftOuterJoin(payCategoryId2CountRDD).mapToPair(
                new PairFunction<Tuple2<Long, Tuple2<String, com.google.common.base.Optional<Long>>>, Long, String>() {
                    @Override
                    public Tuple2<Long, String> call(Tuple2<Long, Tuple2<String, com.google.common.base.Optional<Long>>> tup) throws Exception {
                        long categoryId = tup._1;
                        String value = tup._2._1;
                        com.google.common.base.Optional<Long> optional = tup._2._2;

                        long payCount = 0L;

                        if (optional.isPresent()) {
                            payCount = optional.get();
                        }

                        value = value + "|" + Constants.FIELD_PAY_COUNT + "=" + payCount;

                        return new Tuple2<Long, String>(categoryId, value);
                    }
                });
        return tmpMapRDD;
    }


    /**
     * 计算各品类的支付次数
     * @param sessionId2DetailRDD
     * @return
     */
    private static JavaPairRDD<Long, Long> getPayCategoryId2CountRDD(
            JavaPairRDD<String, Row> sessionId2DetailRDD) {

        // 过滤支付字段值为空的数据
        JavaPairRDD<String, Row> payActionRDD = sessionId2DetailRDD.filter(new Function<Tuple2<String, Row>, Boolean>() {
            @Override
            public Boolean call(Tuple2<String, Row> tup) throws Exception {
                Row row = tup._2;
                return row.getString(10) != null ? true : false;
            }
        });

        // 生成元组，便于聚合
        JavaPairRDD<Long, Long> payCategoryIdRDD = payActionRDD.flatMapToPair(
                new PairFlatMapFunction<Tuple2<String, Row>, Long, Long>() {
                    @Override
                    public Iterable<Tuple2<Long, Long>> call(Tuple2<String, Row> tup) throws Exception {
                        Row row = tup._2;
                        String payCategoryIds = row.getString(10);
                        String[] payCategoryIdsSplited = payCategoryIds.split(",");

                        // 用于存储切分后的数据: (orderCategoryId, 1L)
                        List<Tuple2<Long, Long>> list = new ArrayList<Tuple2<Long, Long>>();
                        for (String payCategoryId : payCategoryIdsSplited) {
                            list.add(new Tuple2<Long, Long>(Long.valueOf(payCategoryId), 1L));
                        }

                        return list;
                    }
                });

        // 聚合
        JavaPairRDD<Long, Long> payCategoryId2CountRDD =
                payCategoryIdRDD.reduceByKey(new Function2<Long, Long, Long>() {
                    @Override
                    public Long call(Long v1, Long v2) throws Exception {
                        return v1 + v2;
                    }
                });

        return payCategoryId2CountRDD;
    }

    /**
     * 计算各品类的下单次数
     * @param sessionId2DetailRDD
     * @return
     */
    private static JavaPairRDD<Long,Long> getOrderCategoryId2CountRDD(
            JavaPairRDD<String, Row> sessionId2DetailRDD) {
        //过滤下单字段为空的数据
        JavaPairRDD<String,Row> orderActionRDD = sessionId2DetailRDD.filter(new Function<Tuple2<String, Row>, Boolean>() {
            @Override
            public Boolean call(Tuple2<String, Row> stringRowTuple2) throws Exception {
                Row row = stringRowTuple2._2;
                return row.getString(8) != null ? true:false;
            }
        });
        //生成元组，便于聚合
        JavaPairRDD<Long,Long> orderCategoryIdRDD = orderActionRDD.flatMapToPair(
                new PairFlatMapFunction<Tuple2<String, Row>, Long, Long>() {
                    @Override
                    public Iterable<Tuple2<Long, Long>> call(Tuple2<String, Row> stringRowTuple2) throws Exception {
                        Row row = stringRowTuple2._2;
                        String orderCategoryIds = row.getString(8);
                        String[] orderCategoryIdsSplited = orderCategoryIds.split(",");
                        //用于存储切分后的数据：orderCategoryId,1L
                        List<Tuple2<Long,Long>> list = new ArrayList<Tuple2<Long, Long>>();
                        for (String orderCategoryId : orderCategoryIdsSplited) {
                            list.add(new Tuple2<Long, Long>(Long.valueOf(orderCategoryId), 1L));
                        }
                        return list;
                    }
                });
        // 聚合
        JavaPairRDD<Long, Long> orderCategoryId2CountRDD =
                orderCategoryIdRDD.reduceByKey(new Function2<Long, Long, Long>() {
                    @Override
                    public Long call(Long v1, Long v2) throws Exception {
                        return v1 + v2;
                    }
                });

        return orderCategoryId2CountRDD;
    }


    /**
     * 计算各品类的点击次数
     * @param sessionId2DetailRDD
     * @return
     */

    private static JavaPairRDD<Long,Long> getClickCategoryId2CountRDD(
            JavaPairRDD<String, Row> sessionId2DetailRDD) {
        //把明细数据中的点击品类字段的空字段过滤掉
        JavaPairRDD<String,Row>clickActionRDD = sessionId2DetailRDD.filter(new Function<Tuple2<String, Row>, Boolean>() {
            @Override
            public Boolean call(Tuple2<String, Row> stringRowTuple2) throws Exception {
                Row row = stringRowTuple2._2;
                return row.get(6) != null ? true : false;
            }
        });
        //将每一个点击品类后面跟一个1，生成元组:(clickCategoryId,1),为了做聚合
        JavaPairRDD<Long,Long> clickCategoryIdRDD = clickActionRDD.mapToPair(
                new PairFunction<Tuple2<String, Row>, Long, Long>() {
                    @Override
                    public Tuple2<Long, Long> call(Tuple2<String, Row> stringRowTuple2) throws Exception {
                        long clickCategoryId = stringRowTuple2._2.getLong(6);
                        return new Tuple2<Long,Long>(clickCategoryId,1L);
                    }
                });
        //计算各品类的点击次数
        JavaPairRDD<Long,Long> clickCategoryId2CountRDD =
                clickCategoryIdRDD.reduceByKey(new Function2<Long, Long, Long>() {
                    @Override
                    public Long call(Long aLong, Long aLong2) throws Exception {
                        return aLong+aLong2;
                    }
                });
        return clickCategoryId2CountRDD;
    }

    /**
     * 按照时间比例随机抽取session
     * @param sc
     * @param taskid
     * @param filteredSessionId2AggrInfoRDD
     * @param sessionId2DetailRDD
     */
    private static void randomExtranctSession(
            JavaSparkContext sc,
            final long taskid,
            JavaPairRDD<String, String> filteredSessionId2AggrInfoRDD,
            JavaPairRDD<String, Row> sessionId2DetailRDD) {
        /**
         * 第一步：计算出每个小时的session数量
         */
        // 首先需要把数据调整为: <date_hour, data>
        JavaPairRDD<String, String> time2SessionIdRDD =
                filteredSessionId2AggrInfoRDD.mapToPair(
                        new PairFunction<Tuple2<String, String>, String, String>() {
                            @Override
                            public Tuple2<String, String> call(Tuple2<String, String> tup) throws Exception {
                                // 获取聚合数据
                                String aggrInfo = tup._2;
                                // 再从聚合数据里拿到startTime
                                String startTime = StringUtils.getFieldFromConcatString(
                                        aggrInfo, "\\|", Constants.FIELD_START_TIME);

                                // 获取日期和时间(date_hour)
                                String dateHour = DateUtils.getDateHour(startTime);

                                return new Tuple2<String, String>(dateHour, aggrInfo);
                            }
                        });

        // 需要得到每天每小时的session数量，然后计算出每天每小时session抽取索引，遍历每天每小时的session
        // 首先抽取出session聚合数据，写入数据库表：session_random_extract
        // time2SessionIdRDD的数据,是每天的某个小时的session聚合数据

        // 计算每天每小时的session数量
        Map<String, Object> countMap = time2SessionIdRDD.countByKey();

        /**
         * 第二步：使用时间比例随机抽取算法，计算出每天每小时抽取的session索引
         */
        HashMap<String,Map<String,Long>> dateHourCountMap = new HashMap<String, Map<String, Long>>();
        for (Map.Entry<String,Object> countEntry:countMap.entrySet()){
            String dateHour = countEntry.getKey();
            String date = dateHour.split("_")[0];
            String hour = dateHour.split("_")[1];

            //取出每小时的count
            long count = Long.valueOf(String.valueOf(countEntry.getValue()));
            //用来存储<hour,Long>
            Map<String,Long> hourCountMap = dateHourCountMap.get(date);
            if (hourCountMap == null){
                hourCountMap = new HashMap<String, Long>();
                dateHourCountMap.put(date,hourCountMap);
            }
            hourCountMap.put(hour,count);
        }
       //实现时间比例抽取算法
        //从数据中抽取100个session，先按照天数进行平分
       int extractNumber = 100/dateHourCountMap.size();
       //Map<date,Map<hour,list(2,13,4,7,...)>>
        Map<String,Map<String,List<Integer>>>  dateHourExtractMap = new HashMap<String, Map<String, List<Integer>>>();

        Random random = new Random();

        for (Map.Entry<String,Map<String,Long>> dateHourCountEntry:dateHourCountMap.entrySet()){
            String date = dateHourCountEntry.getKey();
            Map<String,Long> hourCountMap = dateHourCountEntry.getValue();

            Long sessionCount = 0L;
            for (long hourCount:hourCountMap.values()){
                sessionCount += hourCount;
            }
            //把一天的session数量put到datehourExtractMap
            Map<String,List<Integer>> hourExtractMap = dateHourExtractMap.get(date);

            if (hourExtractMap == null){
                hourExtractMap = new HashMap<String, List<Integer>>();
                dateHourExtractMap.put(date,hourExtractMap);
            }

            //遍历每个小时的session
            for (Map.Entry<String,Long> hourCountEntry : hourCountMap.entrySet()){
                String hour = hourCountEntry.getKey();
                long count = hourCountEntry.getValue();
                System.out.println(hour+count);

                //计算每个小时在当天占session数量占比，乘以要抽取的数量
                //最后计算出当前小时需要抽取的session数量
                int hourExtractNumber =(int)(((double)count / (double)sessionCount) * extractNumber);

                //当前抽取的session数量有可能大于每小时session数量
                //让当亲小时要抽取的session数量等于每小时session数量
                if (hourExtractNumber > count){
                    hourExtractNumber = (int)count;
                }

                //获取当前小时的存放随机数的list。如果没有就创建一个
                 List<Integer> extractIndexList = hourExtractMap.get(hour);
                if (extractIndexList == null){
                    extractIndexList = new ArrayList<Integer>();
                    hourExtractMap.put(hour,extractIndexList);
                }

                //生成的随机数不能重复
                for (int i = 0;i <hourExtractNumber;i++){
                    int extractIndex = random.nextInt((int)count);//生成随机索引
                    while (extractIndexList.contains(extractIndex)){
                        //如果有重复的随机索引，就重新生成随机数
                        extractIndex = random.nextInt((int)count);
                    }
                    extractIndexList.add(extractIndex);
                }
            }
        }
        // 把dateHourExtractMap封装到fastUtilDateHourExtractMap中(占用内存小,减少网络IO)
        // fastUtil可以封装Map,List,Set等,相比较Java中自带的占用内存更小,在分布式计算中减少网络IO量,传输速度快,占用的网络带宽更小
        Map<String, Map<String, IntList>> fastUtilDateHourExtractMap = new HashMap<>();
        for (Map.Entry<String, Map<String, List<Integer>>> dateHourExtractEntry : dateHourExtractMap.entrySet()) {
            // 日期
            String date = dateHourExtractEntry.getKey();
            // <hour,extract>
            Map<String, List<Integer>> hourExtractMap = dateHourExtractEntry.getValue();
            // 用于存放<hour,extract>
            Map<String, IntList> fastUtilHourExtractMap = new HashMap<>();
            for (Map.Entry<String, List<Integer>> hourExtractEntry : hourExtractMap.entrySet()) {
                // 小时
                String hour = hourExtractEntry.getKey();
                // <extract>
                List<Integer> extractList = hourExtractEntry.getValue();
                // 封装
                IntList fastUtilExtractList = new IntArrayList();
                for (int i = 0; i < extractList.size(); i++) {
                    fastUtilExtractList.add(extractList.get(i));
                }
                fastUtilHourExtractMap.put(hour, fastUtilExtractList);
            }
            fastUtilDateHourExtractMap.put(date, fastUtilHourExtractMap);
        }
        // 集群执行task时,有可能多个Executor会远程回去上面的Map值
        // 这样会产生大量的网络IO,此时最好用广播变量到每一个参与计算的Executor
        final Broadcast<Map<String, Map<String, IntList>>> dateHourExtractMapBroadcast = sc.broadcast(fastUtilDateHourExtractMap);
        /**第三步:遍历每天每小时的Session,根据随机抽取的索引开始抽取*/
        // 需要获取到<dateHour,<session,aggrInfo>>
        JavaPairRDD<String, Iterable<String>> time2SessionRDD = time2SessionIdRDD.groupByKey();
        // 遍历每天每小时的Session
        // 如果发现某个Session正好在指定的这天这个小时的随机索引上
        // 将该Session写入到数据库
        // 然后将抽取出来的Session返回,生成新的JavaRDD<String>
        // 用抽取出来的SessionId,去join他们的访问明细,在写入数据库表
        JavaPairRDD<String, String> extractSessionIdsRDD = time2SessionRDD.flatMapToPair(new PairFlatMapFunction<Tuple2<String, Iterable<String>>, String, String>() {
            @Override
            public Iterable<Tuple2<String, String>> call(Tuple2<String, Iterable<String>> stringIterableTuple2) throws Exception {
                // 用来存储<sessionId>
                List<Tuple2<String, String>> extractSessionIds = new ArrayList<>();
                String dateHour = stringIterableTuple2._1;
                String date = dateHour.split("_")[0];
                String hour = dateHour.split("_")[1];
                Iterator<String> iterator = stringIterableTuple2._2.iterator();
                // 调用广播过来的值
                Map<String, Map<String, IntList>> dateHourExtractMap = dateHourExtractMapBroadcast.getValue();
                // 获取抽取索引List
                IntList extractIndexList = dateHourExtractMap.get(date).get(hour);
                ISessionRandomExtractDAO sessionRandomExtractDAO = DAOFactory.getSessionRandomExtractDAO();
                int index = 0;
                while (iterator.hasNext()) {
                    String sessionAggrInfo = iterator.next();
                    if (extractIndexList.contains(index)) {
                        String sessionId = StringUtils.getFieldFromConcatString(sessionAggrInfo, "\\|", Constants.FIELD_SESSION_ID);
                        // 将数据存入数据库
                        SessionRandomExtract sessionRandomExtract = new SessionRandomExtract();
                        sessionRandomExtract.setTaskid(taskid);
                        sessionRandomExtract.setSessionid(sessionId);
                        sessionRandomExtract.setStartTime(StringUtils.getFieldFromConcatString(sessionAggrInfo, "\\|", Constants.FIELD_START_TIME));
                        sessionRandomExtract.setSearchKeywords(StringUtils.getFieldFromConcatString(sessionAggrInfo, "\\|", Constants.FIELD_SEARCH_KEYWORDS));
                        sessionRandomExtract.setClickCategoryIds(StringUtils.getFieldFromConcatString(sessionAggrInfo, "\\|", Constants.FIELD_CLICK_CATEGORY_IDS));
                        sessionRandomExtractDAO.insert(sessionRandomExtract);
                        // 将SessionId放入List
                        extractSessionIds.add(new Tuple2<String, String>(sessionId, sessionId));
                    }
                    index++;
                }
                return extractSessionIds;
            }
        });
        /**第四步:获取抽取出来的Session对应的明细并存入数据库*/


        // 把明细join出来
        JavaPairRDD<String, Tuple2<String, Row>> extractSessionDetailRDD = extractSessionIdsRDD.join(sessionId2DetailRDD);
        extractSessionDetailRDD.foreachPartition(new VoidFunction<Iterator<Tuple2<String, Tuple2<String, Row>>>>() {
            @Override
            public void call(Iterator<Tuple2<String, Tuple2<String, Row>>> tuple2Iterator) throws Exception {
                // 用来存储明细数据的List
                List<SessionDetail> sessionDetails = new ArrayList<>();
                while (tuple2Iterator.hasNext()) {
                    Tuple2<String, Tuple2<String, Row>> tuple = tuple2Iterator.next();
                    Row row = tuple._2._2;
                    SessionDetail sessionDetail = new SessionDetail();
                    sessionDetail.setTaskid(taskid);
                    sessionDetail.setUserid(row.getLong(1));
                    sessionDetail.setSessionid(row.getString(2));
                    sessionDetail.setPageid(row.getLong(3));
                    sessionDetail.setActionTime(row.getString(4));
                    sessionDetail.setSearchKeyword(row.getString(5));
                    sessionDetail.setClickCategoryId(row.getLong(6));
                    sessionDetail.setClickProductId(row.getLong(7));
                    sessionDetail.setOrderCategoryIds(row.getString(8));
                    sessionDetail.setOrderProductIds(row.getString(9));
                    sessionDetail.setPayCategoryIds(row.getString(10));
                    sessionDetail.setPayProductIds(row.getString(11));
                    sessionDetails.add(sessionDetail);
                }
                ISessionDetailDAO sessionDetailDAO = DAOFactory.getSessionDetailDAO();
                sessionDetailDAO.insertBatch(sessionDetails);
            }
        });
    }

    /**
     * 计算出各个范围的session占比，并写入数据库
     * date 2018.08.1
     * @param value
     * @param taskid
     */
    private static void calculateAndPersistAggrStat(String value, long taskid) {
        // 首先从Accumulator统计的字符串结果中获取各个值
        long session_count = Long.valueOf(StringUtils.getFieldFromConcatString(value, "\\|", Constants.SESSION_COUNT));
        long visit_length_1s_3s = Long.valueOf(StringUtils.getFieldFromConcatString(value, "\\|", Constants.TIME_PERIOD_1s_3s));
        long visit_length_4s_6s = Long.valueOf(StringUtils.getFieldFromConcatString(value, "\\|", Constants.TIME_PERIOD_4s_6s));
        long visit_length_7s_9s = Long.valueOf(StringUtils.getFieldFromConcatString(value, "\\|", Constants.TIME_PERIOD_7s_9s));
        long visit_length_10s_30s = Long.valueOf(StringUtils.getFieldFromConcatString(value, "\\|", Constants.TIME_PERIOD_10s_30s));
        long visit_length_30s_60s = Long.valueOf(StringUtils.getFieldFromConcatString(value, "\\|", Constants.TIME_PERIOD_30s_60s));
        long visit_length_1m_3m = Long.valueOf(StringUtils.getFieldFromConcatString(value, "\\|", Constants.TIME_PERIOD_1m_3m));
        long visit_length_3m_10m = Long.valueOf(StringUtils.getFieldFromConcatString(value, "\\|", Constants.TIME_PERIOD_3m_10m));
        long visit_length_10m_30m = Long.valueOf(StringUtils.getFieldFromConcatString(value, "\\|", Constants.TIME_PERIOD_10m_30m));
        long visit_length_30m = Long.valueOf(StringUtils.getFieldFromConcatString(value, "\\|", Constants.TIME_PERIOD_30m));

        long step_length_1_3 = Long.valueOf(StringUtils.getFieldFromConcatString(value, "\\|", Constants.STEP_PERIOD_1_3));
        long step_length_4_6 = Long.valueOf(StringUtils.getFieldFromConcatString(value, "\\|", Constants.STEP_PERIOD_4_6));
        long step_length_7_9 = Long.valueOf(StringUtils.getFieldFromConcatString(value, "\\|", Constants.STEP_PERIOD_7_9));
        long step_length_10_30 = Long.valueOf(StringUtils.getFieldFromConcatString(value, "\\|", Constants.STEP_PERIOD_10_30));
        long step_length_30_60 = Long.valueOf(StringUtils.getFieldFromConcatString(value, "\\|", Constants.STEP_PERIOD_30_60));
        long step_length_60 = Long.valueOf(StringUtils.getFieldFromConcatString(value, "\\|", Constants.STEP_PERIOD_60));

        // 计算各个访问时长和访问步长的范围占比
        double visit_length_1s_3s_ratio = NumberUtils.formatDouble((double)visit_length_1s_3s / (double) session_count, 2);
        double visit_length_4s_6s_ratio = NumberUtils.formatDouble((double)visit_length_4s_6s / (double) session_count, 2);
        double visit_length_7s_9s_ratio = NumberUtils.formatDouble((double)visit_length_7s_9s / (double) session_count, 2);
        double visit_length_10s_30s_ratio = NumberUtils.formatDouble((double)visit_length_10s_30s / (double) session_count, 2);
        double visit_length_30s_60s_ratio = NumberUtils.formatDouble((double)visit_length_30s_60s / (double) session_count, 2);
        double visit_length_1m_3m_ratio = NumberUtils.formatDouble((double)visit_length_1m_3m / (double) session_count, 2);
        double visit_length_3m_10m_ratio = NumberUtils.formatDouble((double)visit_length_3m_10m / (double) session_count, 2);
        double visit_length_10m_30m_ratio = NumberUtils.formatDouble((double)visit_length_10m_30m / (double) session_count, 2);
        double visit_length_30m_ratio = NumberUtils.formatDouble((double)visit_length_30m / (double) session_count, 2);

        double step_length_1_3_ratio = NumberUtils.formatDouble((double)step_length_1_3 / (double) session_count, 2);
        double step_length_4_6_ratio = NumberUtils.formatDouble((double)step_length_4_6 / (double) session_count, 2);
        double step_length_7_9_ratio = NumberUtils.formatDouble((double)step_length_7_9 / (double) session_count, 2);
        double step_length_10_30_ratio = NumberUtils.formatDouble((double)step_length_10_30 / (double) session_count, 2);
        double step_length_30_60_ratio = NumberUtils.formatDouble((double)step_length_30_60 / (double) session_count, 2);
        double step_length_60_ratio = NumberUtils.formatDouble((double)step_length_60 / (double) session_count, 2);

        // 将统计结果封装到Domain对象里
        SessionAggrStat sessionAggrStat = new SessionAggrStat();
        sessionAggrStat.setTaskid(taskid);
        sessionAggrStat.setSession_count(session_count);
        sessionAggrStat.setVisit_length_1s_3s_ratio(visit_length_1s_3s_ratio);
        sessionAggrStat.setVisit_length_4s_6s_ratio(visit_length_4s_6s_ratio);
        sessionAggrStat.setVisit_length_7s_9s_ratio(visit_length_7s_9s_ratio);
        sessionAggrStat.setVisit_length_10s_30s_ratio(visit_length_10s_30s_ratio);
        sessionAggrStat.setVisit_length_30s_60s_ratio(visit_length_30s_60s_ratio);
        sessionAggrStat.setVisit_length_1m_3m_ratio(visit_length_1m_3m_ratio);
        sessionAggrStat.setVisit_length_3m_10m_ratio(visit_length_3m_10m_ratio);
        sessionAggrStat.setVisit_length_10m_30m_ratio(visit_length_10m_30m_ratio);
        sessionAggrStat.setVisit_length_30m_ratio(visit_length_30m_ratio);
        sessionAggrStat.setStep_length_1_3_ratio(step_length_1_3_ratio);
        sessionAggrStat.setStep_length_4_6_ratio(step_length_4_6_ratio);
        sessionAggrStat.setStep_length_7_9_ratio(step_length_7_9_ratio);
        sessionAggrStat.setStep_length_10_30_ratio(step_length_10_30_ratio);
        sessionAggrStat.setStep_length_30_60_ratio(step_length_30_60_ratio);
        sessionAggrStat.setStep_length_60_ratio(step_length_60_ratio);

        // 结果存储
        ISessionAggrStatDAO sessionAggrStatDAO = DAOFactory.getSessionAggrStatDAO();
        sessionAggrStatDAO.insert(sessionAggrStat);
    }

    /**
     * 获取通过筛选条件的session的访问明细数据
     * @param filteredSessionId2AggrInfoRDD
     * @param sessionId2ActionRDD
     * @return
     */
    private static JavaPairRDD<String, Row> getSessionId2DetailRDD(
            JavaPairRDD<String, String> filteredSessionId2AggrInfoRDD,
            JavaPairRDD<String, Row> sessionId2ActionRDD) {

        // 得到sessionId对应的按照使用者条件过滤后的明细数据
        JavaPairRDD<String, Row> sessionId2DetailRDD =
                filteredSessionId2AggrInfoRDD.join(sessionId2ActionRDD)
                        .mapToPair(new PairFunction<Tuple2<String, Tuple2<String, Row>>, String, Row>() {
                            @Override
                            public Tuple2<String, Row> call(Tuple2<String, Tuple2<String, Row>> tup) throws Exception {

                                return new Tuple2<String, Row>(tup._1, tup._2._2);
                            }
                        });


        return sessionId2DetailRDD;
    }


    /**
     * 按照使用者条件过滤session数据并进行聚合
     *
     * @param sessionId2AggrInfoRDD
     * @param taskParam
     * @param sessionAggrStatAccumulator
     * @return
     */
    private static JavaPairRDD<String, String> filteredSessionAndAggrStat(
            JavaPairRDD<String, String> sessionId2AggrInfoRDD,
            JSONObject taskParam,
            final Accumulator<String> sessionAggrStatAccumulator) {

        // 先把所有筛选条件提取出来并拼接为一条字符串
        String startAge = ParamUtils.getParam(taskParam, Constants.PARAM_START_AGE);
        String endAge = ParamUtils.getParam(taskParam, Constants.PARAM_END_AGE);
        String professionals = ParamUtils.getParam(taskParam, Constants.PARAM_PROFESSIONALS);
        String cities = ParamUtils.getParam(taskParam, Constants.PARAM_CITIES);
        String sex = ParamUtils.getParam(taskParam, Constants.PARAM_SEX);
        String keywords = ParamUtils.getParam(taskParam, Constants.PARAM_KEYWORDS);
        String categorys = ParamUtils.getParam(taskParam, Constants.PARAM_CATEGORY_IDS);

        String _parameter = (startAge != null ? Constants.PARAM_START_AGE + "=" + startAge + "|" : "")
                + (endAge != null ? Constants.PARAM_END_AGE + "=" + endAge + "|" : "")
                + (professionals != null ? Constants.PARAM_PROFESSIONALS + "=" + professionals + "|" : "")
                + (cities != null ? Constants.PARAM_CITIES + "=" + cities + "|" : "")
                + (sex != null ? Constants.PARAM_SEX + "=" + sex + "|" : "")
                + (keywords != null ? Constants.PARAM_KEYWORDS + "=" + keywords + "|" : "")
                + (categorys != null ? Constants.PARAM_CATEGORY_IDS + "=" + categorys + "|" : "");

        // 把_parameter的值的最后一个“|”截取掉
        if (_parameter.endsWith("\\|")) {
            _parameter = _parameter.substring(0, _parameter.length() - 1);
        }

        final String parameter = _parameter;

        // 根据筛选条件进行过滤
        JavaPairRDD<String, String> filteredSessionAggrInfoRDD =
                sessionId2AggrInfoRDD.filter(new Function<Tuple2<String, String>, Boolean>() {
                    @Override
                    public Boolean call(Tuple2<String, String> tup) throws Exception {
                        // 从tup中获取基础数据
                        String aggrInfo = tup._2;

                        /**
                         * 依次按照筛选条件进行过滤
                         */
                        // 按照年龄进行过滤
                        if (!ValidUtils.between(aggrInfo, Constants.FIELD_AGE, parameter,
                                Constants.PARAM_START_AGE, Constants.PARAM_END_AGE)) {
                            return false;
                        }

                        // 按照职业进行过滤
                        if (!ValidUtils.in(aggrInfo, Constants.FIELD_PROFESSIONAL, parameter,
                                Constants.PARAM_PROFESSIONALS)) {
                            return false;
                        }

                        // 按照城市信息进行过滤
                        if (!ValidUtils.in(aggrInfo, Constants.FIELD_CITY, parameter,
                                Constants.PARAM_CITIES)) {
                            return false;
                        }

                        // 按照性别进行过滤
                        if (!ValidUtils.in(aggrInfo, Constants.FIELD_SEX, parameter,
                                Constants.PARAM_SEX)) {
                            return false;
                        }

                        // 按照关键字进行过滤
                        if (!ValidUtils.in(aggrInfo, Constants.FIELD_SEARCH_KEYWORDS, parameter,
                                Constants.PARAM_KEYWORDS)) {
                            return false;
                        }

                        // 按照点击品类进行过滤
                        if (!ValidUtils.in(aggrInfo, Constants.FIELD_CLICK_CATEGORY_IDS, parameter,
                                Constants.PARAM_CATEGORY_IDS)) {
                            return false;
                        }

                        /**
                         * 代码执行到这里，说明该session通过了用户指定的筛选条件
                         * 接下来要对session的访问时长和访问步长进行统计
                         */

                        // 根据session对应的时长和步长的时间范围进行累加操作
                        sessionAggrStatAccumulator.add(Constants.SESSION_COUNT);

                        // 计算出session的访问时长和访问步长的范围并进行累加
                        long visitLength = Long.valueOf(StringUtils.getFieldFromConcatString(
                                aggrInfo, "\\|", Constants.FIELD_VISIT_LENGTH));
                        long stepLength = Long.valueOf(StringUtils.getFieldFromConcatString(
                                aggrInfo, "\\|", Constants.FIELD_STEP_LENGTH));

                        // 计算访问时长范围
                        calculateVisitLength(visitLength);

                        // 计算访问步长范围
                        calculateStepLength(stepLength);

                        return true;
                    }

                    /**
                     * 计算访问步长范围
                     * @param stepLength
                     */
                    private void calculateStepLength(long stepLength) {
                        if (stepLength >= 1 && stepLength <= 3) {
                            sessionAggrStatAccumulator.add(Constants.STEP_PERIOD_1_3);
                        } else if (stepLength >= 4 && stepLength <= 6) {
                            sessionAggrStatAccumulator.add(Constants.STEP_PERIOD_4_6);
                        } else if (stepLength >= 7 && stepLength <= 9) {
                            sessionAggrStatAccumulator.add(Constants.STEP_PERIOD_7_9);
                        } else if (stepLength >= 10 && stepLength < 30) {
                            sessionAggrStatAccumulator.add(Constants.STEP_PERIOD_10_30);
                        } else if (stepLength >= 30 && stepLength < 60) {
                            sessionAggrStatAccumulator.add(Constants.STEP_PERIOD_30_60);
                        } else if (stepLength >= 60) {
                            sessionAggrStatAccumulator.add(Constants.STEP_PERIOD_60);
                        }
                    }

                    /**
                     * 计算访问时长范围
                     * @param visitLength
                     */
                    private void calculateVisitLength(long visitLength) {
                        if (visitLength >= 1 && visitLength <= 3) {
                            sessionAggrStatAccumulator.add(Constants.TIME_PERIOD_1s_3s);
                        } else if (visitLength >= 4 && visitLength <= 6) {
                            sessionAggrStatAccumulator.add(Constants.TIME_PERIOD_4s_6s);
                        }  else if (visitLength >= 7 && visitLength <= 9) {
                            sessionAggrStatAccumulator.add(Constants.TIME_PERIOD_7s_9s);
                        } else if (visitLength >= 10 && visitLength < 30) {
                            sessionAggrStatAccumulator.add(Constants.TIME_PERIOD_10s_30s);
                        } else if (visitLength >= 30 && visitLength < 60) {
                            sessionAggrStatAccumulator.add(Constants.TIME_PERIOD_30s_60s);
                        } else if (visitLength >= 60 && visitLength < 180) {
                            sessionAggrStatAccumulator.add(Constants.TIME_PERIOD_1m_3m);
                        } else if (visitLength >= 180 && visitLength < 600) {
                            sessionAggrStatAccumulator.add(Constants.TIME_PERIOD_3m_10m);
                        } else if (visitLength >= 600 && visitLength < 1800) {
                            sessionAggrStatAccumulator.add(Constants.TIME_PERIOD_10m_30m);
                        } else if (visitLength >= 1800) {
                            sessionAggrStatAccumulator.add(Constants.TIME_PERIOD_30m);
                        }
                    }
                });

        return filteredSessionAggrInfoRDD;
    }


    private static JavaPairRDD<String,String> aggrgateBySession(
            JavaSparkContext sc,
            SQLContext sqlContext,
            JavaPairRDD<String, Row> sessionId2ActionRDD){

        // 对行为数据进行分组
        JavaPairRDD<String, Iterable<Row>> sessionId2ActionPairRDD =
                sessionId2ActionRDD.groupByKey();

        // 对每个session分组进行聚合，将session中所有的搜索关键字和点击品类都聚合起来
        // 格式：<userId, partAggrInfo(sessionId, searchKeywords, clickCategoryIds, visitLength, stepLength, startTime)>
        JavaPairRDD<Long, String> userId2PartAggrInfoRDD = sessionId2ActionPairRDD.mapToPair(
                new PairFunction<Tuple2<String, Iterable<Row>>, Long, String>() {
                    @Override
                    public Tuple2<Long, String> call(Tuple2<String, Iterable<Row>> tup) throws Exception {
                        String sessionId = tup._1;
                        Iterator<Row> it = tup._2.iterator();

                        // 用来存储搜索关键字和点击品类
                        StringBuffer searchKeywordsBuffer = new StringBuffer();
                        StringBuffer clickCategoryIdsBuffer = new StringBuffer();

                        // 用来存储userId
                        Long userId = null;

                        // 用来存储起始时间和结束时间
                        Date startTime = null;
                        Date endTime = null;

                        // 用来存储session的访问步长
                        int stepLength = 0;

                        // 遍历session中所有的行为数据
                        while (it.hasNext()) {
                            Row row = it.next();
                            if (userId == null) {
                                userId = row.getLong(1);
                            }

                            // 获取每个访问行为的搜索关键字和点击品类
                            // 注意：如果该行为是搜索行为，searchKeyword是有值的
                            // 但同时点击行为就没有值，任何的行为，不可能有两个字段都有值
                            String searchKeyword = row.getString(5);
                            String clickCategoryId = String.valueOf(row.getLong(6));

                            // 追加搜关键字
                            if (!StringUtils.isEmpty(searchKeyword)) {
                                if (!searchKeywordsBuffer.toString().contains(searchKeyword)) {
                                    searchKeywordsBuffer.append(searchKeyword + ",");
                                }
                            }

                            // 追加点击品类
                            if (clickCategoryId != null) {
                                if (!clickCategoryIdsBuffer.toString().contains(clickCategoryId)) {
                                    clickCategoryIdsBuffer.append(clickCategoryId + ",");
                                }
                            }

                            // 计算session的开始时间和结束时间
                            Date actionTime = DateUtils.parseTime(row.getString(4));
                            if (startTime == null) {
                                startTime = actionTime;
                            }
                            if (endTime == null) {
                                endTime = actionTime;
                            }
                            if (actionTime.before(startTime)) {
                                startTime = actionTime;
                            }
                            if (actionTime.after(endTime)) {
                                endTime = actionTime;
                            }

                            // 计算访问步长
                            stepLength++;
                        }

                        // 截取字符串中两端的“，”， 得到搜索关键字和点击品类
                        String searchKeywords = StringUtils.trimComma(searchKeywordsBuffer.toString());
                        String clickCategoryIds = StringUtils.trimComma(clickCategoryIdsBuffer.toString());

                        // 计算访问时长，单位为秒
                        long visitLenth = (endTime.getTime() - startTime.getTime()) / 1000;

                        // 聚合数据，数据以字符串拼接的方式：key=value|key=value|key=value
                        String partAggrInfo = Constants.FIELD_SESSION_ID + "=" + sessionId + "|"
                                + Constants.FIELD_SEARCH_KEYWORDS + "=" + searchKeywords + "|"
                                + Constants.FIELD_CLICK_CATEGORY_IDS + "=" + clickCategoryIds + "|"
                                + Constants.FIELD_VISIT_LENGTH + "=" + visitLenth + "|"
                                + Constants.FIELD_STEP_LENGTH + "=" + stepLength + "|"
                                + Constants.FIELD_START_TIME + "=" + DateUtils.formatTime(startTime) + "|";

                        return new Tuple2<Long, String>(userId, partAggrInfo);
                    }
                });

        // 查询所有用户数据，构建成<userId, Row>格式
        String sql = "select * from user_info";
        JavaRDD<Row> userInfoRDD = sqlContext.sql(sql).javaRDD();
        JavaPairRDD<Long, Row> userId2InfoRDD = userInfoRDD.mapToPair(new PairFunction<Row, Long, Row>() {
            @Override
            public Tuple2<Long, Row> call(Row row) throws Exception {
                return new Tuple2<Long, Row>(row.getLong(0), row);
            }
        });

        // 将session粒度的聚合数据和用户信息进行join， 格式为：<userId, <sessionInfo, userInfo>>
        JavaPairRDD<Long, Tuple2<String, Row>> userId2FullInfoRDD =
                userId2PartAggrInfoRDD.join(userId2InfoRDD);

        // 对join后的数据进行重新拼接，返回格式为：<sessionId, fullAggrInfo>
        JavaPairRDD<String, String> sessionId2FullAggrInfoRDD = userId2FullInfoRDD.mapToPair(
                new PairFunction<Tuple2<Long, Tuple2<String, Row>>, String, String>() {
                    @Override
                    public Tuple2<String, String> call(Tuple2<Long, Tuple2<String, Row>> tup) throws Exception {
                        // 获取sessionId对应的聚合数据
                        String partAggrInfo = tup._2._1;
                        // 获取用户信息
                        Row userInfoRow = tup._2._2;

                        // 获取sessionId
                        String sessoinId = StringUtils.getFieldFromConcatString(
                                partAggrInfo, "\\|", Constants.FIELD_SESSION_ID);

                        // 提取用户信息的age
                        int age = userInfoRow.getInt(3);

                        // 提取用户信息的职业
                        String professional = userInfoRow.getString(4);

                        // 提取用户信息的所在城市
                        String city = userInfoRow.getString(5);

                        // 提取用户信息的性别
                        String sex = userInfoRow.getString(6);

                        // 拼接
                        String fullAggrInfo = partAggrInfo
                                + Constants.FIELD_AGE + "=" + age + "|"
                                + Constants.FIELD_PROFESSIONAL + "=" + professional + "|"
                                + Constants.FIELD_CITY + "=" + city + "|"
                                + Constants.FIELD_SEX + "=" + sex + "|";

                        return new Tuple2<String, String>(sessoinId, fullAggrInfo);
                    }
                });

        return sessionId2FullAggrInfoRDD;
    }

                /**
                 * 获取sessionId对应的行为数据，生成session力度的数据
                 *
                 */
    private static JavaPairRDD<String,Row> getSessionId2ActionRDD(JavaRDD<Row> actionRDD){
        return actionRDD.mapPartitionsToPair(new PairFlatMapFunction<Iterator<Row>, String, Row>() {
            @Override
            public Iterable<Tuple2<String, Row>> call(Iterator<Row> it) throws Exception {
                //封装基础数据
                ArrayList<Tuple2<String, Row>> list = new ArrayList<Tuple2<String, Row>>();
                while (it.hasNext()) {
                    Row row = it.next();
                    list.add(new Tuple2<String, Row>(row.getString(2), row));
                }

                return list;
            }

        });
    }

}

/**
 * 作业2018-08-15
 * 作业：
 1、整理今天的代码，保证能够跑通
 2、把今天实现的需求的整个逻辑思路用文字的形式写出来
 3、能够依据文字思路实现代码
 4、总结开发经验（把需求转换为代码的过程）
 */

























