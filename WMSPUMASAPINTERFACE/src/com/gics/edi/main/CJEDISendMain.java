package com.gics.edi.main;

import java.sql.Connection;
import java.util.ArrayList;

import org.apache.log4j.Logger;
import com.gics.edi.CJEDI_SEND;
import com.gics.edi.conn.CJEDIConnManager;

public class CJEDISendMain {
	private static Logger logger  = Logger.getLogger("process.lo");
	private static Logger loggerErr = Logger.getLogger("process.err");
	public static void main(String[] args) {
		
		CJEDIConnManager manager = null;
		Connection conn = null;
		Connection connCJ = null;
		CJEDI_SEND sender = null;
		int result = -1;
		try {		
			logger.info("SendLo Start !");
			manager = CJEDIConnManager.getInstance(); //DB접속매니저 생성(싱글톤)
			conn = manager.connectREAL(); // WMS쪽 접속객체
			connCJ = manager.connectCJ(); // CJ쪽 접속객체
						
			//connCJ = manager.connectTest();
			sender = new CJEDI_SEND(); // 출고확정정보 송신모듈생성
			
			/* 출고 송신대상 업데이트
			 * WMS쪽 접속객체 넘김
			 * 업데이트된 행의 개수가 리턴됨
			 * 업데이트된 행의 개수가 1보다 작으면 트랜젝션 롤백
			 */
			
			result = sender.updateTarget(conn); 
			if(result < 1) {
				conn.rollback();
				connCJ.rollback();
				logger.info("");
				return;
			}
			
			ArrayList<String[]> arrLoData = sender.selectLoData(conn); //출고확정데이터 조회
			if(arrLoData.size() < 1) {
				conn.rollback();
				connCJ.rollback();
				logger.info("SendLo Failed !");
				return;
			}
			
			result = sender.runCJ(connCJ, arrLoData); // 해당데이터의 주소부분을 CJ쪽에 접속하여 함수를 이용해 100자 자른 후 insert
			if(result < 1) {
				conn.rollback();
				connCJ.rollback();
				logger.info("SendLo Failed !");
				return;
			}
			
			result = sender.insertLoData(conn, arrLoData); //표준화된 주소를 이용해 WMS insert
			if(result < 1) {
				conn.rollback();
				connCJ.rollback();
				logger.info("SendLo Failed !");
				return;
			}
			
			result = sender.updateTargetDone(conn);
			if(result < 1) {
				conn.rollback();
				connCJ.rollback();
				logger.info("SendLo Failed !");
				return;
			}
			
			if(result > 0) { //여기까지 result가 0보다 크면 커밋
				conn.commit();
				connCJ.commit();
				logger.info("SendLo Completed !");
				
//				int tmpResult = sender.insertSMS(conn, "출고확정정보 송신 완료 -->"+arrLoData.size()+"건");
//				if(tmpResult > 0) {
//					conn.commit();
//				}
			} else { // 이 경우는 발생할 수 없으나 예외처리를 위해 추가
				conn.rollback();
				connCJ.rollback();
				logger.info("SendLo Failed !");
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
