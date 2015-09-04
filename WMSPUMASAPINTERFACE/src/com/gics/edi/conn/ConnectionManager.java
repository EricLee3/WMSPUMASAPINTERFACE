package com.gics.edi.conn;

import java.sql.Connection;
import java.sql.DriverManager;
import java.util.Properties;
import org.apache.log4j.Logger;
import com.gics.edi.common.PropManager;


public class ConnectionManager {
	private static ConnectionManager connMgr;
	private static Logger logger  = Logger.getLogger(ConnectionManager.class);
	private Logger loggerErr = Logger.getLogger("process.err");
	private ConnectionManager() {}
	public static ConnectionManager getInstances() {
		if(connMgr == null) {
			connMgr = new ConnectionManager();
		}
		return connMgr;
	}
	
	public Connection connect(String propName) {
		logger.info("Connection start!");
		Connection conn = null;
		PropManager propMgr = PropManager.getInstances();
		Properties prop = propMgr.getProp(propName);
		
		String url= prop.getProperty("jdbc.url");
		String driver = prop.getProperty("jdbc.driver");
		String userId = prop.getProperty("jdbc.userId");
		String userPw = prop.getProperty("jdbc.userPw");
		
		logger.debug("url: "+url);
		logger.debug("driver: "+driver);
		logger.debug("userId: "+userId);
		logger.debug("userPw: "+userPw);
				
		try {
			Class.forName(driver);
			conn= DriverManager.getConnection(url ,userId, userPw);
			conn.setAutoCommit(false);

		} catch (Exception e) {
			e.printStackTrace();
			loggerErr.error(e.getMessage());
		}
		logger.info("Connection end!");
		return conn;
	}
}
