package com.github.myretail.sql4es.esjdbc;

import nl.anchormen.esjdbc.Sql4EsBase;
import nl.anchormen.sql4es.model.Utils;
import org.elasticsearch.test.ESIntegTestCase.ClusterScope;
import org.elasticsearch.test.ESIntegTestCase.Scope;
import org.junit.Test;

import java.sql.*;

@ClusterScope(scope=Scope.TEST, numDataNodes=1)
public class MySimpleSelectsIT extends Sql4EsBase {

	private String index = "item_sku_income";
	private String type = "sku_income";

	public MySimpleSelectsIT() throws Exception {
		super();
	}
	

	@Test
	public void selectAll() throws Exception{
//		createIndexTypeWithDocs(index, type, true, 10);
		Statement st = DriverManager.getConnection("jdbc:sql4es://es.test.standino.com:9303/"+index+"?cluster.name=jiesi-1").createStatement();
		ResultSet rs = st.executeQuery("select * from "+type+" limit 5");
		ResultSetMetaData rsm = rs.getMetaData();

		
		int count = 0;
		while(rs.next()){
			count++;

			System.out.println(rs.getObject("sale_amount"));

		}

	}
	

		
}
