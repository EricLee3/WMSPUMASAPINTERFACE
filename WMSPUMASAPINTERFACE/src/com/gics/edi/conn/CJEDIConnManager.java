package com.gics.edi.conn;

import java.sql.Connection;
import java.sql.DriverManager;
import org.apache.log4j.Logger;
import com.gics.edi.CJEDI_SEND;

public class CJEDIConnManager {
	
	private static Logger logger  = Logger.getLogger(CJEDI_SEND.class);
	private Logger loggerErr = Logger.getLogger("process.err");
	private static CJEDIConnManager manager;
	private CJEDIConnManager() {}
	public static CJEDIConnManager getInstance() {
		if(manager == null) {
			manager = new CJEDIConnManager();
		}	
		return manager;
	}
	
	public Connection connectCJ() {
		logger.info("CJ DB Connect start !");
		System.out.println("CJ DB Connect start !");
		Connection conn = null;
		String url= "jdbc:oracle:thin:@210.98.159.153:1521:OPENDB";
		try {
			Class.forName("oracle.jdbc.driver.OracleDriver");
			conn= DriverManager.getConnection(url ,"isec", "isec!@#$");
			conn.setAutoCommit(false);
			
			System.out.println("CJ DB Connected !");
			logger.info("CJ DB Connected !");
		} catch (Exception e) {
			logger.error("CJ DB Connect failed !");
			System.out.println("CJ DB Connect failed !");
			loggerErr.error(e.getMessage());
			System.exit(-1);
		}
		
		return conn;
	}
	
	public Connection connectCJtest() {
		logger.info("CJ TEST DB Connect start !");
		Connection conn = null;
		String url= "jdbc:oracle:thin:@210.98.159.153:1523:OPENDBT";			
		try {
			Class.forName("oracle.jdbc.driver.OracleDriver");
			conn= DriverManager.getConnection(url ,"isec", "isecdev!@#$");
			conn.setAutoCommit(false);
			logger.info("CJ TEST DB Connected !");
		} catch (Exception e) {
			logger.error("CJ TEST DB Connect failed !");
			loggerErr.error(e.getMessage());
			System.exit(-1);
		}
		return conn;
	}

	public Connection connectREAL() {
		logger.info("WMS DB Connect start!");
		System.out.println("WMS DB Connect start!");
		Connection conn = null;
		String url="jdbc:oracle:thin:@220.117.243.54:1522:WMS";
		try {
			Class.forName("oracle.jdbc.driver.OracleDriver");
			conn= DriverManager.getConnection(url ,"WMS_USER", "WMS_PWD");
			conn.setAutoCommit(false);
			System.out.println("WMS DB Connected !");
			logger.info("WMS DB Connected !");
		} catch (Exception e) {
			System.out.println("WMS DB Failed !");
			logger.error("WMS DB Failed !");
			loggerErr.error(e.getMessage());			
			System.exit(-1);
		}
		return conn;
	}
	
	public Connection connectTest() {
		logger.info("WMS TEST DB Connect start!");
		Connection conn = null;
		String url="jdbc:oracle:thin:@220.117.243.55:1521:WMS";
		try {
			Class.forName("oracle.jdbc.driver.OracleDriver");
			conn= DriverManager.getConnection(url ,"WMS_USER", "WMS_USER");
			conn.setAutoCommit(false);
			logger.info("WMS TEST DB Connected !");
		} catch (Exception e) {
			logger.error("WMS TEST DB Failed !");
			loggerErr.error(e.getMessage());
			System.exit(-1);
		}
		return conn;
	}
}
