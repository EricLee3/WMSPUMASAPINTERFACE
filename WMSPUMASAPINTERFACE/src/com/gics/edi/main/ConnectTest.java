package com.gics.edi.main;

import java.sql.Connection;

import org.apache.log4j.Logger;

import com.gics.edi.conn.CJEDIConnManager;

public class ConnectTest {
	private static Logger logger  = Logger.getLogger("process.recv");
	public static void main(String args[]) {
		logger.info("접속테스트 시작");
		Connection conn  = null;
		Connection conn1 = null;
		try {
			CJEDIConnManager connMgr = CJEDIConnManager.getInstance();
			 conn = connMgr.connectCJ();
			 conn1 = connMgr.connectREAL();
			 logger.info("접속테스트 종료");	
		} catch(Exception e) {
			
		} finally {
			try {
				conn.close();
				conn1.close();
			} catch (Exception e1) {
				
			}
		}
	}
}
