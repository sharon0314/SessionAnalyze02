package com.qf.sessionanalyze1706.spark.page;

import com.alibaba.fastjson.JSONObject;
import com.qf.sessionanalyze1706.constant.Constants;
import com.qf.sessionanalyze1706.dao.IPageSplitConvertRateDAO;
import com.qf.sessionanalyze1706.dao.ITaskDAO;
import com.qf.sessionanalyze1706.dao.factory.DAOFactory;
import com.qf.sessionanalyze1706.domain.PageSplitConvertRate;
import com.qf.sessionanalyze1706.domain.Task;
import com.qf.sessionanalyze1706.util.DateUtils;
import com.qf.sessionanalyze1706.util.NumberUtils;
import com.qf.sessionanalyze1706.util.ParamUtils;
import com.qf.sessionanalyze1706.util.SparkUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import scala.Tuple2;

import java.util.*;

/**
 * 页面单挑转化率
 * 1.首先获取按使用者传的页taskParam面流过滤出来的数据
 * 2.生成页面切片，匹配页面流
 * 3.根据页面切片生成的单挑妆化率
 * 4.数据的存储
 */
public class PageOneStepConvert {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setAppName(Constants.SPARK_APP_NAME_PAGE);
        SparkUtils.setMaster(conf);
        JavaSparkContext sc = new JavaSparkContext(conf);
        SQLContext sqlContext = SparkUtils.getSQLContext(sc.sc());

        //本地测试中获取数据
        SparkUtils.mockData(sc,sqlContext);

        //查询任务，获取任务信息
        long taskId = ParamUtils.getTaskIdFromArgs(args,Constants.SPARK_LOCAL_TASKID_PAGE);
        ITaskDAO taskDAO=DAOFactory.getTaskDAO();
        Task task = taskDAO.findById(taskId);
        if (task== null){
            System.out.println(new Date() + "无法获取taskId对应的任务信息");
        }
        JSONObject taskParam = JSONObject.parseObject(task.getTaskParam());
        JavaRDD<Row> actionRDD = SparkUtils.getActionRDDByDateRange(sqlContext,taskParam);


        //
        //
        //
        JavaPairRDD<String,Row> sessionId2ActionRDD = getSessionId2ActionRDD(actionRDD);

        //缓存
        sessionId2ActionRDD = sessionId2ActionRDD.cache();

        //因为需要拿到每个session的行为数据，才可以生成切片
        //所以需要对session粒度的基础数据进行分组
        JavaPairRDD<String,Iterable<Row>> groupedSessionId2ActionRDD =
                sessionId2ActionRDD.groupByKey();

        //最关键一步：每个session单挑页面切片的生成和页面流的匹配算法
        //返回格式<split,1>
        JavaPairRDD<String,Integer> pageSplitRDD = generateAndMAtchPageSplit(sc,groupedSessionId2ActionRDD,taskParam);
        //获取切片的访问量
        Map<String,Object> pageSplitPvMap = pageSplitRDD.countByKey();

        //获取其实页面的访问量
        long startPagePv = getStartPagePv(taskParam,groupedSessionId2ActionRDD);

        //计算目标页面的各个页面的切片的转化率
        //Map<String,Double>:key = 各个页面切片，value=页面切片对应的转化率
        Map<String,Double> convertRateMap = computePageSplitConvertRate(taskParam,pageSplitPvMap,startPagePv);

        //结果持久化
        persistConvertRate(taskId,convertRateMap);



        sc.stop();
    }

    /**
     * 将页面切片转化率存入数据库
     * @param taskId
     * @param convertRateMap
     */
    private static void persistConvertRate(
            long taskId,
            Map<String, Double> convertRateMap) {
        //声明一个buffer用于存储页面对应的切片拼接到buffer里面
        StringBuffer buffer = new StringBuffer();
        for (Map.Entry<String,Double> converRateEntry: convertRateMap.entrySet()){
            //获取切片
            String pageSplit = converRateEntry.getKey();
            //获取转化率
            double convertRate = converRateEntry.getValue();
            //拼接
            buffer.append(pageSplit+"="+convertRate+"|");

        }

        //获取拼接后的切片转化率
        String convertRate = buffer.toString();
        //
        convertRate = convertRate.substring(0,convertRate.length()-1);

        PageSplitConvertRate pageSplitConvertRate = new PageSplitConvertRate();
        pageSplitConvertRate.setTaskid(taskId);
        pageSplitConvertRate.setConvertRate(convertRate);
        IPageSplitConvertRateDAO pageSplitConvertRateDAO = DAOFactory.getPageSplitConvertRateDAO();
        pageSplitConvertRateDAO.insert(pageSplitConvertRate);
    }

    /**
     * 计算页面切片转化率
     * @param taskParam
     * @param pageSplitPvMap
     * @param startPagePv
     * @return
     */
    private static Map<String,Double> computePageSplitConvertRate(
            JSONObject taskParam,
            Map<String, Object> pageSplitPvMap, long startPagePv) {
        //用于存储页面切片对应的转化率
        Map<String,Double> convertRateMap = new HashMap<String,Double>();
        //获取页面流
        String[] targetPages = ParamUtils.getParam(taskParam,Constants.PARAM_TARGET_PAGE_FLOW).split(",");

        long lastPageSplitPv = 0L;

        /**
         * 求转化率
         */
        for (int i = 1;i<targetPages.length;i++){
            //获取页面切片
            String targetPageSplit = targetPages[i-1]+"_"+targetPages[i];
            //获取每个页面切片对应的访问量
            long targetPAgeSplitPv = Long.valueOf(String.valueOf(pageSplitPvMap.get(targetPageSplit)));
            //初始化转化率
            double convertRate = 0.0;
            //生成转化率
            if (i==1){
                convertRate = NumberUtils.formatDouble((double) targetPAgeSplitPv/(double) startPagePv,2);
            }else{
                convertRate = NumberUtils.formatDouble((double) targetPAgeSplitPv/(double) lastPageSplitPv,2);
            }

            convertRateMap.put(targetPageSplit,convertRate);

            lastPageSplitPv = targetPAgeSplitPv;
        }
       return convertRateMap;
    }

    /**
     * 获取页面起始流pv
     * @param taskParam
     * @param groupedSessionId2ActionRDD
     * @return
     */
    private static long getStartPagePv(
            JSONObject taskParam,
            JavaPairRDD<String,
            Iterable<Row>> groupedSessionId2ActionRDD) {
        //拿到页面流
        String targetPageFlow = ParamUtils.getParam(taskParam,Constants.PARAM_TARGET_PAGE_FLOW);
        //从页面流中获取其实页面id
       final Long startPageId = Long.valueOf(targetPageFlow.split(",")[0]);

        JavaRDD<Long> startPageRDD = groupedSessionId2ActionRDD.flatMap(new FlatMapFunction<Tuple2<String, Iterable<Row>>, Long>() {
            @Override
            public Iterable<Long> call(Tuple2<String, Iterable<Row>> stringIterableTuple2) throws Exception {
                //用于存储每个session访问的起始页面的id
                List<Long> list = new ArrayList<Long>();
                //获取行为数据
                Iterator<Row> it = stringIterableTuple2._2.iterator();
                while (it.hasNext()){
                    Row row = it.next();
                    long pageid = row.getLong(3);
                    if (pageid == startPageId){
                        list.add(pageid);
                    }
                }
                return list;
            }
        });
        return startPageRDD.count();
    }

    /**
     * 页面切片的生成和页面流匹配算法
     * @param sc
     * @param groupedSessionId2ActionRDD
     * @param taskParam
     * @return
     */
    private static JavaPairRDD<String,Integer> generateAndMAtchPageSplit(
            JavaSparkContext sc,
            JavaPairRDD<String, Iterable<Row>> groupedSessionId2ActionRDD,
            JSONObject taskParam) {
        //首先获取页面流
        String targetPageFlow = ParamUtils.getParam(taskParam,Constants.PARAM_TARGET_PAGE_FLOW);

        //把目标页面流广播到相应的Executor
        Broadcast<String> targetPageFlowBroadcast = sc.broadcast(targetPageFlow);
        //实现页面匹配流算法
        return groupedSessionId2ActionRDD.flatMapToPair(new PairFlatMapFunction<Tuple2<String, Iterable<Row>>, String, Integer>() {
            @Override
            public Iterable<Tuple2<String, Integer>> call(Tuple2<String, Iterable<Row>> stringIterableTuple2) throws Exception {

                List<Tuple2<String,Integer>> list = new ArrayList<Tuple2<String, Integer>>();
                //获取当前session对应的行为数据
                Iterator<Row> it = stringIterableTuple2._2.iterator();

                String[] targetPages = targetPageFlowBroadcast.value().split(",");

                /**
                 * session访问数据此时已经拿到
                 * 但是默认没有排序
                 * 在实现转化率的时候需要按照时间顺序进行排序
                 */

                //把访问行为数据放到list里，便于排序
                List<Row> rows = new ArrayList<Row>();
                while(it.hasNext()){
                    rows.add(it.next());
                }
                //开始按照时间排序，可以用自定义的排序凡是，也可以用您名内部类
                Collections.sort(rows, new Comparator<Row>() {
                    @Override
                    public int compare(Row o1, Row o2) {
                        String actionTime1 = o1.getString(4);
                        String actionTime2 = o2.getString(4);
                        Date date1 = DateUtils.parseTime(actionTime1);
                        Date date2 = DateUtils.parseTime(actionTime2);

                        return (int)(date1.getTime()-date2.getTime());
                    }
                });

                /**
                 * 生成页面切片，并且和页面流进行匹配
                 *
                 */
                //定义一个上一个页面的id
                Long lastPageId = null;
                //现在拿到的rows里的数据是其中一个sessionId对应的所有行为数据
                for (Row row: rows){
                    long pageId = row.getLong(3);
                    if (lastPageId == null){
                        lastPageId = pageId;
                        continue;
                    }

                    /**
                     * 生成一个页面切片
                     * 比如该用户的请求的页面是1，3，4，7
                     * 上次访问的页面id：lastPageId = 1
                     * 这次请求的页面是3
                     * 生成的页面切片是1_3
                     */
                    String pageSplit = lastPageId+"_"+pageId;
                    //对这个页面切片进行判断，是否在使用者制定的页面流中
                    for (int i = 1;i<targetPages.length;i++){
                        //比如说：用户主子顶的页面流是：
                        //1，2，5，6
                        //遍历的时候从索引1开始，也就是四二个页面开始
                        //这样第一个页面切片就是1_2
                        String targetPageSplit = targetPages[i-1]+"_"+targetPages[i];
                        if (pageSplit.equals(targetPageSplit)){
                            list.add(new Tuple2<String,Integer>(pageSplit,1));
                            break;
                        }
                    }
                    lastPageId = pageId;
                }

                return list;
            }
        });

    }

    /**
     * 生成session粒度数据
     * @param actionRDD 行为数据
     * @return <sessionId,Row></>
     */
    private static JavaPairRDD<String,Row> getSessionId2ActionRDD(
            JavaRDD<Row> actionRDD) {


        return actionRDD.mapToPair(new PairFunction<Row, String, Row>() {
            @Override
            public Tuple2<String, Row> call(Row row) throws Exception {

                return new Tuple2<>(row.getString(2),row);
            }
        });
    }

}
