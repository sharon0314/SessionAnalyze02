package com.qf.sessionanalyze1706.dao;

import com.qf.sessionanalyze1706.domain.AdUserClickCount;

import java.util.List;

/**
 * 用户广告点击量DAO接口
 * @author Administrator
 *
 */
public interface IAdUserClickCountDAO {

	/**
	 * 批量更新用户广告点击量
	 * @param adUserClickCounts
	 */
	void updateBatch(List<AdUserClickCount> adUserClickCounts);
	
	/**
	 * 根据多个key查询用户广告点击量
	 * @param date 日期
	 * @param userid 用户id
	 * @param adid 广告id
	 * @return
	 */
	int findClickCountByMultiKey(String date, long userid, long adid);
	
}
