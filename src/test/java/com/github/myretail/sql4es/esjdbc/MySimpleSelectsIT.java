package com.github.myretail.sql4es.esjdbc;

import nl.anchormen.esjdbc.Sql4EsBase;
import nl.anchormen.sql4es.model.Utils;
import org.elasticsearch.SpecialPermission;
import org.elasticsearch.test.ESIntegTestCase.ClusterScope;
import org.elasticsearch.test.ESIntegTestCase.Scope;
import org.junit.Test;


import java.math.BigDecimal;
import java.sql.*;
import java.text.DecimalFormat;


public class MySimpleSelectsIT  {



	private String index = "item_sku_income";
	private String type = "sku_income";

	private SecurityManager sm;

	/**
	 * Loads the ESDriver
	 * @throws Exception
	 */
	public MySimpleSelectsIT() throws Exception {
		super();
		Class.forName("nl.anchormen.sql4es.jdbc.ESDriver");
		this.sm = System.getSecurityManager();
		if (sm != null) {
			// unprivileged code such as scripts do not have SpecialPermission
			sm.checkPermission(new SpecialPermission());
		}
	}

	@Test

	public void limitTest() throws SQLException {
		String sql = "select  item_third_cate_cd, item_third_cate_name" +
				" from sku_income  limit  20000";

		Statement st = DriverManager.getConnection("jdbc:sql4es://es.test.standino.com:9303/"+index+"?cluster.name=jiesi-1").createStatement();
		ResultSet rs = st.executeQuery(sql);
		ResultSetMetaData rsm = rs.getMetaData();



		int count = 0;
		while(rs.next()){
			count++;
//			System.out.print(rs.getObject("dept_id_2")+"----"+rs.getObject("dept_id_3")+":  ");
//			System.out.println(formatMeasure(rs.getDouble("sale_amount"),"0.##"));

		}

			System.out.println("记录数："+count);
	}



	@Test
	public void selectAll() throws Exception{

		String sql ="select sum(sale_amount)/sum(dept_id_1) as sale_amount, item_second_cate_name, item_third_cate_name" +
				" from sku_income , query_cache group by item_second_cate_name, item_third_cate_name";

        sql ="select sum(sale_amount) as sale_amount, item_second_cate_name, item_third_cate_name " +
                "from (select sale_amount, item_second_cate_name, item_third_cate_name from  sku_income where item_second_cate_name='大 家 电') as sku_income " +
                "group by item_second_cate_name, item_third_cate_name";


		sql = "select sum(sale_amount)  as sale_amount, dept_id_1, dept_id_2, dept_id_3" +
				" from sku_income where dept_id_2 = 37 group by dept_id_1, dept_id_2, dept_id_3";
//		sql="select * from "+type+" limit 5";

		Statement st = DriverManager.getConnection("jdbc:sql4es://es.test.standino.com:9303/"+index+"?cluster.name=jiesi-1").createStatement();
		ResultSet rs = st.executeQuery(sql);
		ResultSetMetaData rsm = rs.getMetaData();



		int count = 0;
		while(rs.next()){
			count++;
            System.out.print(rs.getObject("dept_id_2")+"----"+rs.getObject("dept_id_3")+":  ");
			System.out.println(formatMeasure(rs.getDouble("sale_amount"),"0.##"));

		}

	}

	protected String formatMeasure(Double value, String format) {

		DecimalFormat df = new DecimalFormat(format);

		//去除垃圾数据
		if (value==null) {
			return "";
		}
		try {
			return df.format(new BigDecimal(value));
		} catch (Exception e) {
			System.out.println("格式化指标出错:" + value);
		}
		return value.toString();

	}
	

		
}
