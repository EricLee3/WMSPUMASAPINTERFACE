package com.gics.edi.main;

import java.sql.Connection;

import org.apache.log4j.Logger;

import com.gics.edi.CJEDI_RECV_AREA;
import com.gics.edi.conn.CJEDIConnManager;

public class CJEDIAreaMain {
	private static Logger logger  = Logger.getLogger("process.area");
	private static Logger loggerErr = Logger.getLogger("process.err");
	public static void main(String arg[]) {
		CJEDIConnManager manager = null;
		Connection conn = null;
		Connection connCJ = null;
		CJEDI_RECV_AREA cjArea = null;
		try {
			manager = CJEDIConnManager.getInstance();
			conn = manager.connectREAL();
			connCJ = manager.connectCJ();
			
			conn.setAutoCommit(false);
			connCJ.setAutoCommit(false);
			
			logger.debug("CJAREA START --");
			cjArea = new CJEDI_RECV_AREA();
			int result = cjArea.run(connCJ, conn);
			
			if(result < 1) {
				conn.rollback();
				connCJ.rollback();
				logger.debug("CJAREA rollback --");
			} else {
				conn.commit();
				connCJ.commit();
				logger.debug("CJAREA commit --");
			}
		} catch (Exception e) {
			e.printStackTrace();
			loggerErr.error(e.getMessage());
		} finally {
			try {
				conn.close();
				connCJ.close();
			} catch(Exception e) {
				loggerErr.error(e.getMessage());
			}
			logger.debug("CJAREA finish --");
		}
	}
}
