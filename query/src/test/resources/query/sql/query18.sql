SELECT 
 test_cal_dt.week_beg_dt 
 ,test_category_groupings.meta_categ_name 
 ,sum(price) as GMV, count(*) as TRANS_CNT 
 FROM test_kylin_fact 
 inner JOIN edw.test_cal_dt as test_cal_dt  
 ON test_kylin_fact.cal_dt = test_cal_dt.cal_dt 
 inner JOIN test_category_groupings 
 ON test_kylin_fact.leaf_categ_id = test_category_groupings.leaf_categ_id AND test_kylin_fact.lstg_site_id = test_category_groupings.site_id 
 where test_cal_dt.week_beg_dt between DATE '2013-09-01' and DATE '2013-10-01' 
 and test_category_groupings.categ_lvl2_name='Comics' 
 group by test_cal_dt.week_beg_dt 
 ,test_category_groupings.meta_categ_name 
 
