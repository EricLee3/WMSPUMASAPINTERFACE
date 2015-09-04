package com.gics.edi.main;

import java.sql.Connection;

import org.apache.log4j.Logger;

import com.gics.edi.CJEDI_RECV;
import com.gics.edi.conn.CJEDIConnManager;

public class CJEDIRecvMain {
	private static Logger logger  = Logger.getLogger("process.recv");
	private static Logger loggerErr = Logger.getLogger("process.err");
	public static void main(String[] args) {
		logger.info("RecvTrace Start !");
		CJEDIConnManager manager = null;
		Connection conn = null;
		Connection connCJ = null;
		CJEDI_RECV recv = null;
		int result = -1;
		try {		
			manager = CJEDIConnManager.getInstance();
			conn = manager.connectREAL();
			connCJ = manager.connectCJ();
			
			//connCJ = manager.connectTest();
			recv = new CJEDI_RECV();
			result = recv.run(connCJ, conn);
			if(result < 1) {
				conn.rollback();
			}
			if(result > 0) {
				conn.commit();
				connCJ.commit();
				logger.info("RecvTrace Completed");
			}
		} catch (Exception e) {
			try {
				conn.rollback();
				connCJ.rollback();
			} catch (Exception e1) {
				loggerErr.error(e.getMessage());
			}
			loggerErr.error(e.getMessage());
			
			try {
				int tmpResult = recv.insertSMS(conn, e.getMessage());
				if(tmpResult > 0) {
					conn.commit();
				}
			} catch (Exception e1) {
				loggerErr.error(e1.getMessage());
			}
			e.printStackTrace();
			System.exit(-1);
		} finally {
			try {
				conn.close();
				connCJ.close();
			} catch(Exception e) {
				loggerErr.error(e.getMessage());
			}
			System.exit(0);
		}
	}
}
