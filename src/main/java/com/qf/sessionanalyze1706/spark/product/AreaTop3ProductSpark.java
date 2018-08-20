package com.qf.sessionanalyze1706.spark.product;

import com.alibaba.fastjson.JSONObject;
import com.qf.sessionanalyze1706.constant.Constants;
import com.qf.sessionanalyze1706.dao.ITaskDAO;
import com.qf.sessionanalyze1706.dao.factory.DAOFactory;
import com.qf.sessionanalyze1706.domain.Task;
import com.qf.sessionanalyze1706.util.ParamUtils;
import com.qf.sessionanalyze1706.util.SparkUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import scala.Tuple2;

/**
 * 区域热门top3
 * 1.按照指定日期范围内的点击行为数据
 * 2.按照城市信息
 * 3.按照区域分组排序
 * 4.得到区域top3热门商品
 * 5.持久化
 */
public class AreaTop3ProductSpark {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setAppName(Constants.SPARK_APP_NAME_PRODUCT);
        SparkUtils.setMaster(conf);
        JavaSparkContext sc = new JavaSparkContext(conf);
        SQLContext sqlContext = SparkUtils.getSQLContext(sc.sc());

        //注册自定义函数

        //本地测试中获取数据
        SparkUtils.mockData(sc,sqlContext);

        //获取taskId 查询到相应的任务信息
        long taskId = ParamUtils.getTaskIdFromArgs(args,Constants.SPARK_APP_NAME_PRODUCT);
        ITaskDAO taskDAO = DAOFactory.getTaskDAO();
        Task task = taskDAO.findById(taskId);
        JSONObject taskParam = JSONObject.parseObject(task.getTaskParam());

        //
        String startDate = ParamUtils.getParam(taskParam,Constants.PARAM_START_DATE);
        String endDate = ParamUtils.getParam(taskParam,Constants.PARAM_END_DATE);

        JavaPairRDD<Long,Row> cityId2ClickActionRDD = getCityId2ClickActionRDD(sqlContext,startDate,endDate);

    }

    /**
     *
     * @param sqlContext
     * @param startDate
     * @param endDate
     * @return
     */
    private static JavaPairRDD<Long,Row> getCityId2ClickActionRDD(
            SQLContext sqlContext,
            String startDate,
            String endDate) {
        //从user_visit_action表中查询用户访问行为数据
        //第一个限定：click_product_id 限定为不为空的访问行为，这个字段的值就代表商品行为
        //第二个限定：在使用者指定日期范围内的数据
        String sql = "select"+
                "city_id,"+
                "click_product_id product_id, "+
                "from user_visit_action"+
                "while click_product_id is not null "+
                "and session_date>="+startDate+"'"+
                "and session_date<="+endDate+"'";
        DataFrame clickActionDF = sqlContext.sql(sql);
        //把生成的DataFrame转化为RDD
        JavaRDD<Row> clickActionRDD = clickActionDF.javaRDD();

        JavaPairRDD<Long,Row> cityId2ActionRDD = clickActionRDD.mapToPair(new PairFunction<Row, Long, Row>() {
            @Override
            public Tuple2<Long, Row> call(Row row) throws Exception {

                return new Tuple2<>(row.getLong(0),row);
            }
        });

        return null;
    }
}
