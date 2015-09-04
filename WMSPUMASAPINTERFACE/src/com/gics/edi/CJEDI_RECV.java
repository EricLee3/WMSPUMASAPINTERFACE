package com.gics.edi;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.ArrayList;

import org.apache.log4j.Logger;

public class CJEDI_RECV {
	private Logger logger  = Logger.getLogger("process.recv");
	private Logger loggerErr = Logger.getLogger("process.err");
 	private PreparedStatement pstmt;
 	private ResultSet rs;
 	
 	public int run(Connection connCJ, Connection conn) {
 		int result = -1;
 		StringBuilder sb = new StringBuilder();
		try {
			sb.append(" SELECT CUST_ID, CUST_USE_NO, RCPT_DV, CUST_MGMT_DLCM_CD ") 
			  .append(" FROM EDIEXPRESS_CJ_SEND ")  
			  .append(" MINUS ")
			  .append(" SELECT CUST_ID, CUST_USE_NO, RCPT_DV, CUST_MGMT_DLCM_CD ") 
			  .append(" FROM EDIEXPRESS_CJ_RECEIVE ")
			  .append(" WHERE CRG_ST = '91' ")
			  .append(" GROUP BY  CUST_ID, CUST_USE_NO, RCPT_DV, CUST_MGMT_DLCM_CD ");
			
			pstmt = conn.prepareStatement(sb.toString());
			rs = pstmt.executeQuery();
			sb.delete(0, sb.length());
			
			ArrayList<String[]> recvTargetData = new ArrayList<String[]>();
			while(rs.next()) {
				String[] row = new String[4];
				row[0] = rs.getString("CUST_ID");
				row[1] = rs.getString("CUST_USE_NO");
				row[2] = rs.getString("RCPT_DV");
				row[3] = rs.getString("CUST_MGMT_DLCM_CD");
				
				recvTargetData.add(row);
			}
			
			logger.debug("recvTargetData size: "+ recvTargetData.size());
			ArrayList<String[]> cjRows = new ArrayList<String[]>();
			sb.append(" SELECT  SERIAL, CUST_ID, RCPT_DV, INVC_NO, CUST_USE_NO ")
			  .append(" , CUST_MGMT_DLCM_CD, CRG_ST, SCAN_YMD, SCAN_HOUR, DEALT_BRAN_NM ")
			  .append(" ,DEALT_BRAN_TEL, DEALT_EMP_NM, DEALT_EMP_TEL, ACPTR_NM, NO_CLDV_RSN_CD ")
			  .append(" ,DETAIL_RSN, EAI_PRGS_ST, EAI_ERR_MSG, REG_EMP_ID, MODI_EMP_ID ")
	 		  .append(" FROM    V_TRACE_ISEC025 ")
	 		  .append(" WHERE CUST_ID = ? ")
	 		  .append(" AND CUST_USE_NO = ? ")
	 		  .append(" AND RCPT_DV = ? ")
	 		  .append(" AND CUST_MGMT_DLCM_CD = ? ");
			
			logger.debug(sb.toString());
			pstmt = connCJ.prepareStatement(sb.toString());
			for(int i=0; i<recvTargetData.size(); i++) {
				
	 			pstmt.setString(1, recvTargetData.get(i)[0]);
	 			pstmt.setString(2, recvTargetData.get(i)[1]);
	 			pstmt.setString(3, recvTargetData.get(i)[2]);
	 			pstmt.setString(4, recvTargetData.get(i)[3]);
	 			
	 			rs = pstmt.executeQuery();
	 			while(rs.next()) {
	 				String[] row = new String[20]; 
	 				row[0] = rs.getString("SERIAL");
	 				row[1] = rs.getString("CUST_ID");
	 				row[2] = rs.getString("RCPT_DV");
	 				row[3] = rs.getString("INVC_NO");
	 				row[4] = rs.getString("CUST_USE_NO");
	 				row[5] = rs.getString("CUST_MGMT_DLCM_CD");
	 				row[6] = rs.getString("CRG_ST");
	 				row[7] = rs.getString("SCAN_YMD");
	 				row[8] = rs.getString("SCAN_HOUR");
	 				row[9] = rs.getString("DEALT_BRAN_NM");
	 				row[10] = rs.getString("DEALT_BRAN_TEL");
	 				row[11] = rs.getString("DEALT_EMP_NM");
	 				row[12] = rs.getString("DEALT_EMP_TEL");
	 				row[13] = rs.getString("ACPTR_NM");
	 				row[14] = rs.getString("NO_CLDV_RSN_CD");
	 				row[15] = rs.getString("DETAIL_RSN");
	 				row[16] = rs.getString("EAI_PRGS_ST");
	 				row[17] = rs.getString("EAI_ERR_MSG");
	 				row[18] = rs.getString("REG_EMP_ID");
	 				row[19] = rs.getString("MODI_EMP_ID");
	 				cjRows.add(row);
	 			}
			}
			
 			result = insertEDI(conn, cjRows);
 			if(result < 1) {
 				logger.info(" run insertIntoEDI :: No data !");
 				return result;
			}
 			
 			result = updateOrderRT(conn, cjRows);
 			if(result < 1) {
 				logger.info(" run insertIntoEDI :: No data !");
			}
 			
 			result = updateExpressDate(conn);
 			if(result < 1) {
 				logger.info(" run insertIntoEDI :: No data !");
			}
 			
		}  catch (Exception e) {
			//e.printStackTrace();
			try {
				conn.rollback();
				connCJ.rollback();
			} catch (Exception e1) {
				loggerErr.error(e1.getMessage());
			}
			loggerErr.error(e.getMessage());
			insertSMS(conn, e.getMessage());
		}  finally {
			try {
				close();
			} catch (Exception e) {
				e.printStackTrace();
				loggerErr.error(e.getMessage());
			}
		}
		return result;
 	}
 	
 	private int insertEDI(Connection conn, ArrayList<String[]> cjRows) {
 		int result = -1;
 		StringBuilder sb = new StringBuilder();
 		try {
 	 		sb.append(" SELECT 1 ")
 	 		  .append(" FROM EDIEXPRESS_CJ_RECEIVE ")
 	 		  .append(" WHERE SERIAL = ? ")
 	 		  .append(" AND SERIAL = ? ")
 	 		  .append(" AND CUST_ID = ? ")
 	 		  .append(" AND RCPT_DV = ? ")
 	 		  .append(" AND INVC_NO = ? ")
 	 		  .append(" AND CUST_USE_NO = ? ")
 	 		  .append(" AND CUST_MGMT_DLCM_CD = ? ")
 	 		  .append(" AND CRG_ST = ? ");
 	 		pstmt = conn.prepareStatement(sb.toString());
 	 		for(int i=0; i<cjRows.size(); i++) {
 	 			pstmt.setString(1, cjRows.get(i)[0]);
 	 			pstmt.setString(2, cjRows.get(i)[1]);
 	 			pstmt.setString(3, cjRows.get(i)[2]);
 	 			pstmt.setString(4, cjRows.get(i)[3]);
 	 			pstmt.setString(5, cjRows.get(i)[4]);
 	 			pstmt.setString(6, cjRows.get(i)[5]);
 	 			pstmt.setString(7, cjRows.get(i)[6]);
 	 			pstmt.setString(8, cjRows.get(i)[7]);
 	 			
 	 			rs = pstmt.executeQuery();
 	 			if(rs.next()) {
 	 				cjRows.remove(i);
 	 				i--;
 	 			}
 	 		}
 	 		
 	 		logger.debug("cjRows size: "+cjRows.size());
 	 		logger.debug("======== cjRows START ========");
 	 		for(int i=0; i<cjRows.size(); i++) {
 	 			for(int j=0; j<cjRows.get(i).length; j++) {
 	 				logger.debug(cjRows.get(i)[j]+" , ");
 	 			}
 	 			logger.debug("\n");
 	 		}
 	 		logger.debug("======== cjRows END ========");
 	 		 			
 	 		sb.delete(0, sb.length());
 			sb.append(" INSERT INTO EDIEXPRESS_CJ_RECEIVE ")
			  .append(" VALUES (   ?, ? , ? , ? , ? , ? , ? , ? , ? , ? , ? , ? , ? , ? , ? , ? , ? , ? , ? , SYSDATE , ? , SYSDATE ) ");
 			pstmt = conn.prepareStatement(sb.toString());
 			for(int i=0; i<cjRows.size(); i++) {
	 			pstmt.setString(1, cjRows.get(i)[0]);
	 			pstmt.setString(2, cjRows.get(i)[1]);
	 			pstmt.setString(3, cjRows.get(i)[2]);
	 			pstmt.setString(4, cjRows.get(i)[3]);
	 			pstmt.setString(5, cjRows.get(i)[4]);
	 			pstmt.setString(6, cjRows.get(i)[5]);
	 			pstmt.setString(7, cjRows.get(i)[6]);
	 			pstmt.setString(8, cjRows.get(i)[7]);
	 			pstmt.setString(9, cjRows.get(i)[8]);
	 			pstmt.setString(10, cjRows.get(i)[9]);
	 			pstmt.setString(11, cjRows.get(i)[10]);
	 			pstmt.setString(12, cjRows.get(i)[11]);
	 			pstmt.setString(13, cjRows.get(i)[12]);
	 			pstmt.setString(14, cjRows.get(i)[13]);
	 			pstmt.setString(15, cjRows.get(i)[14]);
	 			pstmt.setString(16, cjRows.get(i)[15]);
	 			pstmt.setString(17, cjRows.get(i)[16]);
	 			pstmt.setString(18, cjRows.get(i)[17]);
	 			pstmt.setString(19, cjRows.get(i)[18]);
	 			pstmt.setString(20, cjRows.get(i)[19]);
	 			
	 			result = pstmt.executeUpdate();
	 			if(result < 1) {
	 				break;
	 			}
	 		}
 		} catch (Exception e) {
			 try {
				 result = -1;
				 conn.rollback();
			 } catch (Exception e1) {
			//	e1.printStackTrace(); 
				 result = -1;
				 loggerErr.error(e1.getMessage());
			 } 
			// e.printStackTrace();
			 loggerErr.error(e.getMessage());
			 insertSMS(conn, e.getMessage());
 		}
 		return result;
 	}
 	
 	private int updateExpressDate(Connection conn) {
 		int result = -1;
 		StringBuilder sb = new StringBuilder();
 		sb.append(" UPDATE /*+ BYPASS_UJVC */ ")
 		.append(" ( ")
 		.append(" SELECT A.EXPRESS_DATE, B.SCAN_YMD ")
 		.append(" FROM ")
 		.append(" ( ")
 		.append(" SELECT * ") 
 		.append(" FROM ORDER_RETURN_DATA ") 
 		.append(" WHERE EXPRESS_DATE IS NULL AND EXPRESS_NO IS NOT NULL ")
 		.append(" ) A , (  SELECT INVC_NO, SCAN_YMD ")
        .append(" FROM EDIEXPRESS_CJ_RECEIVE ") 
        .append(" WHERE  CRG_ST = '91' ")
        .append(" GROUP BY INVC_NO,  SCAN_YMD ") 
        .append(" ) B ")
        .append(" WHERE A.EXPRESS_NO = B.INVC_NO ")        
        .append(" ) ")
        .append(" SET EXPRESS_DATE = SCAN_YMD ");
 		try {
 			pstmt = conn.prepareStatement(sb.toString());
 			result = pstmt.executeUpdate();
 			if(result < 1) {
 				logger.info(" updateExpressData :: No data !");
			}
 		} catch (Exception e) {
 			e.printStackTrace();
 			loggerErr.error(e.getMessage());
		} 
 		return result;
 	}
 	
 	private int updateOrderRT(Connection conn, ArrayList<String[]> cjRows) {
 		int result = -1;
 		StringBuilder sb = new StringBuilder();
 		String centerCd = "";
		String brandCd = "";
		String orderDate = "";
		String orderNo = "";
		String expressNo = "";
		try {
			pstmt = conn.prepareStatement(" ");
			for(int i=0; i<cjRows.size(); i++) {
				if(cjRows.get(i)[4].length()<18 ||
						cjRows.get(i)[2].equals("01")) {
					continue;
				}
				
				expressNo = cjRows.get(i)[3];
				centerCd = cjRows.get(i)[4].substring(0, 2);
				brandCd = cjRows.get(i)[4].substring(2, 6);
				orderDate = cjRows.get(i)[4].substring(6, 14);
				orderNo = cjRows.get(i)[4].substring(14, 18);
				
				sb.append(" UPDATE ORDER_RETURN_DATA ")
				  .append(" SET EXPRESS_NO = '").append(expressNo).append("'")
				  .append(" WHERE CENTER_CD = '").append(centerCd).append("'")
				  .append(" AND BRAND_CD = '").append(brandCd).append("'")
				  .append(" AND ORDER_DATE = TO_DATE('").append(orderDate).append("','YYYYMMDD')")
				  .append(" AND ORDER_NO = '").append(orderNo).append("'")
				  .append(" AND INOUT_CD = 'E30'");
				
				logger.info("update SQL : "+sb.toString());
				result = pstmt.executeUpdate(sb.toString());
				logger.info("result : "+result);
				sb.delete(0, sb.length());
				if(result < 1) {
	 				logger.info(" updateOrderRT :: No data !");
				}
			}
		}  catch (Exception e) {
			e.printStackTrace();
			try {
				conn.rollback();
			} catch (Exception e1) {
				loggerErr.error(e1.getMessage());
			}
			loggerErr.error(e.getMessage());
			insertSMS(conn, e.getMessage());
		}
		return result; 		
 	}
 	
 	public int insertSMS(Connection conn, String errMsg) {
 		int result = -1;
 		StringBuilder sb = new StringBuilder();
 		ArrayList<String> emergencyContacts = new ArrayList<String>(); //비상연락처
 		//emergencyContacts.add("01023783091"); //이동신 팀장
 		emergencyContacts.add("01022462042"); //박정화
 		errMsg = "WMS RecvTrace Error:: " +errMsg.substring(0, errMsg.indexOf(":", 1));
 		try {
 			pstmt = conn.prepareStatement(sb.toString());
	 		for(int i=0; i<emergencyContacts.size(); i++) {
	 			sb.append(" INSERT /* Order.orderConfirmSMS */ INTO CMSMSG@WIZWID ( ")
	 			  .append(" SEQNUM, ORDER_ID, SMSMSG_GB, DESTIN, SM, CALLBACK, REG_ID, ")
	              .append(" REG_DT, UPD_ID, UPD_DT) ")
	              .append(" VALUES ( ")
	              .append(" CMSMSG_SEQ.nextval@wizwid, ")
	              .append(" '','A0', '").append(emergencyContacts.get(i)).append("' ,")
	              .append(" '").append(errMsg).append("', '07087082872','000000101',")
	              .append(" TO_CHAR (SYSDATE, 'YYYYMMDDHH24MISS'),'000000101', TO_CHAR (SYSDATE, 'YYYYMMDDHH24MISS'))");
	 			result = pstmt.executeUpdate(sb.toString());
	 			if(result < 1) {
	 				break;	 				
	 			}
	 			sb.delete(0, sb.length());
	 		}	
 		} catch (Exception e) {
 			try {
 				conn.rollback();
 			} catch (Exception e1) {
 				loggerErr.error(e1.getMessage());
			}
 			loggerErr.error(e.getMessage());
		} finally {
			try {
				close();
			} catch (Exception e) {
				loggerErr.error(e.getMessage());
			}
		}
 		return result;
 	}
 	
	private void close() throws Exception {
		if(rs != null) {
			rs.close();
		}
		if(pstmt != null) {
			pstmt.close();
		}
	}
}
