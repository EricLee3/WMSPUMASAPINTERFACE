package com.gics.edi.main;

import java.sql.Connection;
import java.util.ArrayList;

import org.apache.log4j.Logger;

import com.gics.edi.CJEDI_SEND_RT;
import com.gics.edi.conn.CJEDIConnManager;

public class CJEDISendRTMain {
	private static Logger logger  = Logger.getLogger("process.lr");
	private static Logger loggerErr = Logger.getLogger("process.err");
	public static void main(String[] args) {
		logger.info("SendLr Start !");
		CJEDIConnManager manager = null;
		Connection conn = null;
		Connection connCJ = null;
		CJEDI_SEND_RT sender = null;
		int result = -1;
		try {		
			manager = CJEDIConnManager.getInstance();
			conn = manager.connectREAL();
			connCJ = manager.connectCJ();
			
			//connCJ =  manager.connectTest();
			sender = new CJEDI_SEND_RT();
			
			result = sender.updateTarget(conn);
			if(result < 1) {
				conn.rollback();
				connCJ.rollback();
				logger.info("SendLr failed !");
				return;
			}
			
			ArrayList<String[]> arrLrData = sender.selectLrData(conn);
			if(arrLrData.size() < 1) {
				conn.rollback();
				connCJ.rollback();
				logger.info("SendLr failed !");
				return;
			}
			
			result = sender.insertRT(conn);
			if(result < 1) {
				conn.rollback();
				connCJ.rollback();
				logger.info("SendLr failed !");
				return;
			}
			
			result = sender.runCJ(connCJ, arrLrData);
			if(result < 1) {
				conn.rollback();
				connCJ.rollback();
				logger.info("SendLr failed !");
				return;
			}
			
			result = sender.insertLrData(conn, arrLrData);
			if(result < 1) {
				conn.rollback();
				connCJ.rollback();
				logger.info("SendLr failed !");
				return;
			}
			
			result = sender.updateTargetDone(conn);
			if(result < 1) {
				conn.rollback();
				connCJ.rollback();
				logger.info("SendLr failed !");
				return;
			}
				
			if(result > 0) {
				conn.commit();
				connCJ.commit();
				logger.info("SendLr Completed !");
				
//				int tmpResult = sender.insertSMS(conn, "반입예정정보 송신 완료 --> "+arrLrData.size()+"건");
//				if(tmpResult > 0) {
//					conn.commit();
//				}
			} else {
				conn.rollback();
				connCJ.rollback();
			}
			
		} catch (Exception e) {
			sender.updateTargetCancel(conn);
			loggerErr.error(e.getMessage());
			
			try {
				int tmpResult = sender.insertSMS(conn, e.getMessage());
				if(tmpResult > 0) {
					conn.commit();
				}
			} catch (Exception e1) {
				logger.error(e1.getMessage());
			}
			
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
