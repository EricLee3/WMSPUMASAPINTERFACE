package com.gics.edi;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.ArrayList;

import org.apache.log4j.Logger;

public class CJEDI_RECV_AREA {
	private Logger logger  = Logger.getLogger("process.area");
	private Logger loggerErr = Logger.getLogger("process.err");
	private PreparedStatement pstmt;
 	private ResultSet rs;
 	
	public int run(Connection connCJ, Connection conn) {
		logger.debug("run start --");
		int result = -1;
		StringBuilder sb = new StringBuilder();
		sb.append(" SELECT * FROM TB_POST010 ");
		ArrayList<String[]> arrAreaInfo = new ArrayList<String[]>();
		try {
			pstmt = connCJ.prepareStatement(sb.toString());
			rs = pstmt.executeQuery(sb.toString());
			while(rs.next()) {
				String[] row = new String[15];
				row[0] = rs.getString("ZIP_NO");
				row[1] = rs.getString("MAN_BRAN_ID");
				row[2] = rs.getString("MAN_BRAN_NM");
				row[3] = rs.getString("UP_BRAN_ID");
				row[4] = rs.getString("UP_BRAN_NM");
				row[5] = rs.getString("SIDO_ADDR");
				row[6] = rs.getString("SKK_ADDR");
				row[7] = rs.getString("DONG_ADDR");
				row[8] = rs.getString("END_NO");
				row[9] = rs.getString("SUB_END_NO");
				row[10] = rs.getString("END_NM");
				row[11] = rs.getString("CLDV_EMP_NM");
				row[12] = rs.getString("FERRY_RGN_YN");
				row[13] = rs.getString("AIR_RGN_YN");
				row[14] = rs.getString("USE_YN");
				
				arrAreaInfo.add(row);
			}
			if(arrAreaInfo.size() > 0) {
				result = insertIntoCMArea(conn,arrAreaInfo);
			} else {
				return -1;
			}
		} catch (Exception e) {
			e.printStackTrace();
			try {
				result = -1;
				conn.rollback();
				connCJ.rollback();
				loggerErr.error(e.getMessage());
			} catch (Exception e1) {
				loggerErr.error(e1.getMessage());
			}
		} finally {
			try {
				close();
			} catch (Exception e) {
				loggerErr.error(e.getMessage());
			}
		}
		logger.debug("run end --");
		return result;
	}
	
	private int insertIntoCMArea(Connection conn, ArrayList<String[]> arrAreaInfo) {
		logger.debug("insertIntoCMArea start --");
		int result = -1;
		try {
			StringBuilder sb = new StringBuilder();
			sb.append(" SELECT NVL(MAX('Y'),'N') ISEXIST FROM TAB WHERE tabtype = 'TABLE' AND TNAME = 'CMEXPRESSAREA_CJ_BACK' ");
			pstmt = conn.prepareStatement(sb.toString());
			rs = pstmt.executeQuery();
			rs.next();
			
			sb.delete(0, sb.length());
			if(rs.getString("ISEXIST").equals("N")) {
				sb.append(" CREATE TABLE CMEXPRESSAREA_CJ_BACK AS ")
				  .append(" SELECT * FROM CMEXPRESSAREA_CJ WHERE 1=0");
				
				pstmt = conn.prepareStatement(sb.toString());
				result = pstmt.executeUpdate();
				if(result < 0) {
					loggerErr.info("CMEXPRESSAREA_CJ_BACK table create failed.");
					return -1;
				}
			}
			
			sb.delete(0, sb.length());
			sb.append(" TRUNCATE TABLE CMEXPRESSAREA_CJ_BACK " );
			pstmt = conn.prepareStatement(sb.toString());
			result = pstmt.executeUpdate();
			if(result < 0) {
				loggerErr.info("CMEXPRESSAREA_CJ_BACK table TRUNCATE failed.");
				return -1;
			}
			
			sb.delete(0, sb.length());
			sb.append(" INSERT INTO CMEXPRESSAREA_CJ_BACK ")
			  .append(" SELECT * FROM CMEXPRESSAREA_CJ ");
			
			pstmt = conn.prepareStatement(sb.toString());
			result = pstmt.executeUpdate();
			if(result < 1) {
				System.out.println("result: "+result);
				loggerErr.info("CMEXPRESSAREA_CJ_BACK table INSERT failed.");
				return -1;
			}	
			conn.commit();
			sb.delete(0, sb.length());
			
			sb.append(" DELETE FROM CMEXPRESSAREA_CJ ");
			pstmt = conn.prepareStatement(sb.toString());
			conn.setAutoCommit(false);
			result = pstmt.executeUpdate();
			if(result < 1) {
				return -1;
			}
			sb.delete(0, sb.length());
			sb.append(" INSERT INTO CMEXPRESSAREA_CJ ( ZIP_NO,") 
			  .append(" MAN_BRAN_ID ")
			  .append(" ,MAN_BRAN_NM ")
			  .append(" ,UP_BRAN_ID ")
			  .append(" ,UP_BRAN_NM ")
			  .append(" ,SIDO_ADDR ")
			  .append(" ,SKK_ADDR ")
			  .append(" ,DONG_ADDR ")
			  .append(" ,END_NO ")
			  .append(" ,SUB_END_NO ")
			  .append(" ,END_NM ")
			  .append(" ,CLDV_EMP_NM ")
			  .append(" ,FERRY_RGN_YN ")
			  .append(" ,AIR_RGN_YN ")
			  .append(" ,USE_YN ")
			  .append(" ,MODI_YMD ")
			  .append(" ,REG_EMP_ID ")
			  .append(" ,REG_DTIME ")
			  .append(" ,MODI_EMP_ID ")
			  .append(" ,MODI_DTIME ) ")
			  .append(" VALUES ( ? , ? , ? , ? , ? , ? , ? , ?, ?, ?, ?, ?, ?, ?, ? ")
			  .append(" , TO_CHAR(SYSDATE,'YYYYMMDD') , 'hwa313' , SYSDATE , 'hwa313' , SYSDATE )");
			pstmt = conn.prepareStatement(sb.toString());
			for(int i=0; i<arrAreaInfo.size(); i++) {
				for(int j=0; j<arrAreaInfo.get(i).length; j++) {
					pstmt.setString(j+1, arrAreaInfo.get(i)[j]);
				}
				result = pstmt.executeUpdate();
				if(result < 1) {
					logger.info("insertIntoCMArea :: No data !!");
					break;
				}
			}
		} catch (Exception e) {
			e.printStackTrace();
			try {
				result = -1;
				conn.rollback();
				loggerErr.error(e.getMessage());
			} catch (Exception e1) {
				loggerErr.error(e1.getMessage());
			}
		}
		logger.debug("insertIntoCMArea finish --");
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
