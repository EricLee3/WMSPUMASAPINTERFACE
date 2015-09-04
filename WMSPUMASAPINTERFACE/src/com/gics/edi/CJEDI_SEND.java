package com.gics.edi;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.util.ArrayList;

import org.apache.log4j.Logger;

public class CJEDI_SEND {
	
	private Logger logger  = Logger.getLogger("process.lo");
	private Logger loggerErr = Logger.getLogger("process.err");
 	private PreparedStatement pstmt;
 	private ResultSet rs;
 	
	public int updateTarget(Connection conn) {
		logger.info("updateTarget :: Start !");
		int result = -1; //업데이트된 행의 개수 --> -1로 초기화
		try {
			StringBuilder sb = new StringBuilder();
			sb.append(" UPDATE LO020NM ")
			.append(" SET FREE_VAL5 = '10' ")
			.append(" WHERE (CENTER_CD ,BRAND_CD ,OUTBOUND_DATE ,OUTBOUND_NO) ")
			.append(" IN ( ")
			.append(" SELECT DISTINCT ")
			.append(" M1.CENTER_CD ,M1.BRAND_CD ")
			.append(" ,M1.OUTBOUND_DATE ")
			.append(" ,M1.OUTBOUND_NO ")
			.append(" FROM    LO020NM             M1 ")
			.append("  JOIN LO020ND        M2  ON  M2.CENTER_CD     = M1.CENTER_CD ")
			.append("   AND M2.BRAND_CD      = M1.BRAND_CD ")
			.append(" AND M2.OUTBOUND_DATE = M1.OUTBOUND_DATE ")
			.append(" AND M2.OUTBOUND_NO   = M1.OUTBOUND_NO ")
			.append(" JOIN LO020NP        M3  ON  M3.CENTER_CD     = M2.CENTER_CD ")
			.append(" AND M3.BRAND_CD      = M2.BRAND_CD ")
			.append(" AND M3.OUTBOUND_DATE = M2.OUTBOUND_DATE ")
			.append(" AND M3.OUTBOUND_NO   = M2.OUTBOUND_NO ")
			.append(" AND M3.LINE_NO       = M2.LINE_NO ")
			.append(" JOIN CMITEM         C1  ON  C1.BRAND_CD      = M2.BRAND_CD ")
			.append(" AND C1.ITEM_CD       = M2.ITEM_CD ")
			.append(" JOIN CMEXPRESSCONST C2  ON  C2.BRAND_CD      = M3.BRAND_CD ")
			.append(" AND C2.CENTER_CD    = M3.CENTER_CD ")
			.append(" AND C2.EXPRESS_CD    = M3.EXPRESS_CD ")
			.append(" JOIN CMBRAND        C3  ON  C3.BRAND_CD      = M1.BRAND_CD ")
			.append(" JOIN CMCUST         C4  ON  C4.CUST_CD       = C3.CUST_CD ")
			.append(" JOIN CMCENTER       C5  ON  C5.CENTER_CD     = M1.CENTER_CD ")
			.append(" JOIN CMDELIVERY     C6  ON  C6.BRAND_CD      = M1.BRAND_CD ")
			.append(" AND C6.DELIVERY_CD   = M1.REAL_DELIVERY_CD ")
			.append(" WHERE   M1.CENTER_CD        LIKE '%' ")
//			.append(" AND     M1.CENTER_CD        NOT IN ('B2') ")
			.append(" AND     M1.BRAND_CD         LIKE '%' ")
			.append(" AND     M1.OUTBOUND_DATE    <= TRUNC(SYSDATE) ")
			.append(" AND     M1.OUTBOUND_DATE    >= TO_DATE('20150508','YYYYMMDD') ")
			.append(" AND     M2.OUTBOUND_STATE   = 50 ")
			.append(" AND     M2.CONFIRM_QTY      > 0 ")
			.append(" AND     (M1.FREE_VAL5       = '00' OR M1.FREE_VAL5 IS NULL) ") // 출고확정정보 대상 
			.append(" AND     C2.EXPRESS_CD       = '09') ");
			
			pstmt = conn.prepareStatement(sb.toString());
			result = pstmt.executeUpdate(); // 업데이트
			if(result > 0) {
				logger.info("updateTarget :: Success !"); // 행의 갯수가 0보다 크면 업데이트 성공
			} else {
				logger.info("updateTarget :: No data !"); // 아니면 노데이터
			}
		} catch(Exception e) {
			loggerErr.error(e.getMessage()); // 에러로그
			try {
				conn.rollback(); // 에러나면 롤백 
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
		logger.info("updateTarget :: End !");
		return result;
	}
	
	public int updateTargetDone(Connection conn) {
		logger.info("updateTargetDone :: Start !");
		int result = -1;
		StringBuilder sb = new StringBuilder();
		try {
			sb.append(" UPDATE LO020NM SET FREE_VAL5 = '60' WHERE FREE_VAL5 = '10' ");
			pstmt = conn.prepareStatement(sb.toString());
			result = pstmt.executeUpdate();
		} catch (Exception e) {
			loggerErr.error(e.getMessage());
		} finally {
			try {
				close();
			} catch (Exception e) {
				loggerErr.error(e.getMessage());
			}
		}
		logger.info("updateTargetDone :: End !");
		return result;
	}
	
	public int updateTargetCancel(Connection conn) {
		logger.info("updateTargetCancel :: Start !");
		StringBuilder sb = new StringBuilder();
		int result = -1;
		try {
			sb.append(" UPDATE LO020NM SET FREE_VAL5 = '00' WHERE FREE_VAL5 = '10' ");
			pstmt = conn.prepareStatement(sb.toString());
			
			result = pstmt.executeUpdate();
		} catch (Exception e) {
			try {
				conn.rollback();
			} catch (Exception e1) {
				loggerErr.error(e.getMessage());
			}
		} finally {
			try {
				close();
			} catch (Exception e) {
				loggerErr.error(e.getMessage());
			}
		}
		logger.info("updateTargetCancel :: End !");
		return result;
	}
	
	public ArrayList<String[]> selectLoData(Connection conn) {
		logger.info("selectLoData :: Start !");
		StringBuilder sb = new StringBuilder();
		ArrayList<String[]> arrLoData = new ArrayList<String[]>();
		ResultSetMetaData rsmd =  null;
		
		try {
			//sb.append(" INSERT INTO EDIEXPRESS_CJ_SEND ").append(" ( ")
			
			sb.append(" SELECT  M1.CENTER_CD ,M1.BRAND_CD ,NVL(C2.CUSTOMER_CD ,' ') AS CUST_ID ")
			.append(" ,TO_CHAR(TO_DATE(SYSDATE) ,'YYYYMMDD')    AS RCPT_YMD ")
			.append(" ,M1.CENTER_CD||M1.BRAND_CD ||TO_CHAR(M1.OUTBOUND_DATE,'YYYYMMDD')||M1.OUTBOUND_NO||M3.BOX_NO AS CUST_USE_NO ")
			.append(" ,DECODE(C7.SUB_CD, 'D1' ,'01' , 'D2' ,'01' , 'D3' ,'02' ,'01') AS RCPT_DV ")
			.append(" ,'01' AS WORK_DV_CD ,'01' AS REQ_DV_CD ,TO_CHAR(TO_DATE(SYSDATE),'YYYYMMDD') ||'_'|| C2.CUSTOMER_CD ||'_'|| M1.BRAND_CD || M2.BRAND_NO AS MPCK_KEY ")
			.append(" ,1 AS MPCK_SEQ ")
			.append(" ,'01' AS CAL_DV_CD ")
			.append(" ,'03' AS FRT_DV_CD ")
			.append(" ,'01' AS CNTR_ITEM_CD ")
			.append(" ,'01' AS BOX_TYPE_CD ")
			.append(" ,1 AS BOX_QTY ")
			.append(" ,0 AS FRT ")
			.append(" ,NVL(C2.JOINT_CD ,' ') AS CUST_MGMT_DLCM_CD ")
			.append(" ,NVL(C2.JOINT_NM ,' ') AS SENDR_NM ")
			.append(" ,CASE WHEN LENGTH(C8.CENTER_TEL_TMP) >= 7 AND LENGTH(C8.CENTER_TEL_TMP) <=12 THEN ")
			.append(" DECODE(LENGTH(C8.CENTER_TEL_TMP),12,SUBSTRB(C8.CENTER_TEL_TMP,1,4) ")
			.append(" ,11,SUBSTRB(C8.CENTER_TEL_TMP,1,3) ")
			.append(" ,10,DECODE(SUBSTRB(C8.CENTER_TEL_TMP,1,2),'02',SUBSTRB(C8.CENTER_TEL_TMP,1,2),SUBSTRB(C8.CENTER_TEL_TMP,1,3)) ")
			.append(" ,9,SUBSTRB(C8.CENTER_TEL_TMP,1,2) ")
			.append(" ,'00') ELSE '00' END AS SENDR_TEL_NO1 ")
			.append(" ,CASE WHEN LENGTH(C8.CENTER_TEL_TMP) >= 7 AND LENGTH(C8.CENTER_TEL_TMP) <=12 THEN ")
			.append(" DECODE(LENGTH(C8.CENTER_TEL_TMP) ,12,SUBSTRB(C8.CENTER_TEL_TMP,5,4) ,11,SUBSTRB(C8.CENTER_TEL_TMP,4,4) ")
			.append(" ,10,DECODE(SUBSTRB(C8.CENTER_TEL_TMP,1,2),'02',SUBSTRB(C8.CENTER_TEL_TMP,3,4),SUBSTRB(C8.CENTER_TEL_TMP,4,3)) ")
			.append(" ,9,SUBSTRB(C8.CENTER_TEL_TMP,3,3) ")
			.append(" ,8,SUBSTRB(C8.CENTER_TEL_TMP,1,4) ")
			.append(" ,7,SUBSTRB(C8.CENTER_TEL_TMP,1,3) ")
			.append(" ,'0000' ) ")
			.append(" ELSE ")
			.append(" '0000' END AS SENDR_TEL_NO2 ")
			.append(" ,CASE WHEN LENGTH(C8.CENTER_TEL_TMP) >= 7 AND LENGTH(C8.CENTER_TEL_TMP) <=12 THEN ")
			.append(" DECODE(LENGTH(C8.CENTER_TEL_TMP),12,SUBSTRB(C8.CENTER_TEL_TMP,9,4),11,SUBSTRB(C8.CENTER_TEL_TMP,8,4) ")
			.append(" ,10,SUBSTRB(C8.CENTER_TEL_TMP,7,4) ,9,SUBSTRB(C8.CENTER_TEL_TMP,6,4) ,8,SUBSTRB(C8.CENTER_TEL_TMP,5,4)")
			.append(" ,7,SUBSTRB(C8.CENTER_TEL_TMP,4,4),'0000') ELSE '0000' END AS SENDR_TEL_NO3 ")
			.append(" ,'' AS SENDR_CELL_NO1 ")
			.append(" ,'' AS SENDR_CELL_NO2 ")
			.append(" ,'' AS SENDR_CELL_NO3 ")
			.append(" ,'' AS SENDR_SAFE_NO1 ")
			.append(" ,'' AS SENDR_SAFE_NO2 ")
			.append(" ,'' AS SENDR_SAFE_NO3 ")
			.append(" ,NVL(C5.ZIP_CD1 ,' ') || NVL(C5.ZIP_CD2 ,' ') AS SENDR_ZIP_NO ")
			.append(" ,REPLACE(C5.ADDR_BASIC,CHR(39),'')  AS SENDR_ADDR ")
			.append(" ,REPLACE(C5.ADDR_BASIC,CHR(39),'')||' '|| REPLACE(NVL(C5.ADDR_DETAIL ,'.'),CHR(39),'') AS SENDR_DETAIL_ADDR")
			.append(" ,NVL(DECODE(M1.REAL_DELIVERY_CD , 'Z999' ,M1.ACPER_NM ,C6.DELIVERY_NM) ,' ') AS RCVR_NM ")
			.append(" ,CASE WHEN LENGTH(C8.ACPER_TEL_TMP) >= 7 AND LENGTH(C8.ACPER_TEL_TMP) <=12 THEN DECODE(LENGTH(C8.ACPER_TEL_TMP) ")
			.append(" ,12,SUBSTRB(C8.ACPER_TEL_TMP,1,4) ,11,SUBSTRB(C8.ACPER_TEL_TMP,1,3) ,10,DECODE(SUBSTRB(C8.ACPER_TEL_TMP,1,2),'02',SUBSTRB(C8.ACPER_TEL_TMP,1,2),SUBSTRB(C8.ACPER_TEL_TMP,1,3)) ")
			.append(" ,9,SUBSTRB(C8.ACPER_TEL_TMP,1,2) ")
			.append(" ,'00') ELSE '00' END AS RCVR_TEL_NO1 ")
			.append(" ,CASE WHEN LENGTH(C8.ACPER_TEL_TMP) >= 7 AND LENGTH(C8.ACPER_TEL_TMP) <=12 THEN DECODE(LENGTH(C8.ACPER_TEL_TMP) ,12,SUBSTRB(C8.ACPER_TEL_TMP,5,4) ,11,SUBSTRB(C8.ACPER_TEL_TMP,4,4) ")
			.append(" ,10,DECODE(SUBSTRB(C8.ACPER_TEL_TMP,1,2),'02',SUBSTRB(C8.ACPER_TEL_TMP,3,4),SUBSTRB(C8.ACPER_TEL_TMP,4,3)),9,SUBSTRB(C8.ACPER_TEL_TMP,3,3),8,SUBSTRB(C8.ACPER_TEL_TMP,1,4) ")
			.append(" ,7,SUBSTRB(C8.ACPER_TEL_TMP,1,3),'0000') ELSE '0000' END AS RCVR_TEL_NO2 ") 
			.append(" ,CASE WHEN LENGTH(C8.ACPER_TEL_TMP) >= 7 AND LENGTH(C8.ACPER_TEL_TMP) <=12 THEN DECODE(LENGTH(C8.ACPER_TEL_TMP) ,12,SUBSTRB(C8.ACPER_TEL_TMP,9,4),11,SUBSTRB(C8.ACPER_TEL_TMP,8,4) ")
			.append(" ,10,SUBSTRB(C8.ACPER_TEL_TMP,7,4) ,9,SUBSTRB(C8.ACPER_TEL_TMP,6,4) ,8,SUBSTRB(C8.ACPER_TEL_TMP,5,4) ,7,SUBSTRB(C8.ACPER_TEL_TMP,4,4) ,'0000' ) ELSE '0000' END AS ")
			.append(" RCVR_TEL_NO3 ")
			.append(" ,CASE WHEN LENGTH(C8.ACPER_HTEL_TMP) >= 7 AND LENGTH(C8.ACPER_HTEL_TMP) <=12 THEN DECODE(LENGTH(C8.ACPER_HTEL_TMP) ,12,SUBSTRB(C8.ACPER_HTEL_TMP,1,4) ")
			.append(" ,11,SUBSTRB(C8.ACPER_HTEL_TMP,1,3),10,DECODE(SUBSTRB(C8.ACPER_HTEL_TMP,1,2),'02',SUBSTRB(C8.ACPER_HTEL_TMP,1,2),SUBSTRB(C8.ACPER_HTEL_TMP,1,3)) ")
			.append(" ,9,SUBSTRB(C8.ACPER_HTEL_TMP,1,2),'00' ) ELSE '00' END AS RCVR_CELL_NO1 ,CASE WHEN LENGTH(C8.ACPER_HTEL_TMP) >= 7 AND LENGTH(C8.ACPER_HTEL_TMP) <=12 THEN ")
			.append(" DECODE(LENGTH(C8.ACPER_HTEL_TMP) ,12,SUBSTR(C8.ACPER_HTEL_TMP,5,4) ,11,SUBSTR(C8.ACPER_HTEL_TMP,4,4) ")
			.append(" ,10,DECODE(SUBSTRB(C8.ACPER_HTEL_TMP,1,2),'02',SUBSTRB(C8.ACPER_HTEL_TMP,3,4),SUBSTRB(C8.ACPER_HTEL_TMP,4,3)) ")
			.append(" ,9,SUBSTRB(C8.ACPER_HTEL_TMP,3,3) ,8,SUBSTRB(C8.ACPER_HTEL_TMP,1,4) ,7,SUBSTRB(C8.ACPER_HTEL_TMP,1,3) ,'0000') ELSE '0000' END AS RCVR_CELL_NO2 ")
			.append(" ,CASE WHEN LENGTH(C8.ACPER_HTEL_TMP) >= 7 AND LENGTH(C8.ACPER_HTEL_TMP) <=12 THEN DECODE(LENGTH(C8.ACPER_HTEL_TMP) ,12,SUBSTRB(C8.ACPER_HTEL_TMP,9,4) ")
			.append(" ,11,SUBSTRB(C8.ACPER_HTEL_TMP,8,4) ,10,SUBSTRB(C8.ACPER_HTEL_TMP,7,4) ,9,SUBSTRB(C8.ACPER_HTEL_TMP,6,4) ,8,SUBSTRB(C8.ACPER_HTEL_TMP,5,4) ")
			.append(" ,7,SUBSTRB(C8.ACPER_HTEL_TMP,4,4),'0000') ELSE '0000' END AS RCVR_CELL_NO3 ")
			.append(" ,'' AS RCVR_SAFE_NO1 ,'' AS RCVR_SAFE_NO2 ,'' AS RCVR_SAFE_NO3 ,NVL(DECODE(M1.REAL_DELIVERY_CD , 'Z999' ,M1.ACPER_ZIP_CD1 ,C6.ZIP_CD1) ,' ') ")
			.append(" || NVL(DECODE(M1.REAL_DELIVERY_CD , 'Z999' ,M1.ACPER_ZIP_CD2 ,C6.ZIP_CD2) ,' ') AS RCVR_ZIP_NO ")
			.append(" ,REPLACE(NVL(DECODE(M1.REAL_DELIVERY_CD , 'Z999' ,M1.ACPER_BASIC ,C6.ADDR_BASIC) ,' '),CHR(39),'') AS RCVR_ADDR ")
			.append(" ,REPLACE(NVL(DECODE(M1.REAL_DELIVERY_CD , 'Z999' ,M1.ACPER_BASIC ,C6.ADDR_BASIC) ,' '),CHR(39),'') ")
			.append("  ||' '|| REPLACE(NVL(DECODE(M1.REAL_DELIVERY_CD , 'Z999' ,M1.ACPER_DETAIL ,C6.ADDR_DETAIL) ,'.'),CHR(39),'') AS RCVR_DETAIL_ADDR ")
			.append(" ,NVL(DECODE(M1.REAL_DELIVERY_CD , 'Z999' ,M1.ORD_NM ,C6.DELIVERY_NM) ,' ') AS ORDRR_NM ")
			.append(" ,CASE ")
			.append(" WHEN LENGTH(C8.ORD_TEL_TMP) >= 7 AND LENGTH(C8.ORD_TEL_TMP) <=12 THEN ")
			.append(" DECODE(LENGTH(C8.ORD_TEL_TMP) ")
			.append(" ,12,SUBSTRB(C8.ORD_TEL_TMP,1,4) ")
			.append(" ,11,SUBSTRB(C8.ORD_TEL_TMP,1,3) ")
			.append(" ,10,DECODE(SUBSTR(C8.ORD_TEL_TMP,1,2),'02',SUBSTRB(C8.ORD_TEL_TMP,1,2),SUBSTRB(C8.ORD_TEL_TMP,1,3)) ")
			.append(" ,9,SUBSTRB(C8.ORD_TEL_TMP,1,2) ")
			.append(" ,'00' ")
			.append(" ) ")
			.append(" ELSE '00' END AS ORDRR_TEL_NO1 ")
			.append(" ,CASE WHEN LENGTH(C8.ORD_TEL_TMP) >= 7 AND LENGTH(C8.ORD_TEL_TMP) <=12 THEN DECODE(LENGTH(C8.ORD_TEL_TMP) ")
			.append(" ,12,SUBSTRB(C8.ORD_TEL_TMP,5,4) ,11,SUBSTRB(C8.ORD_TEL_TMP,4,4) ,10,DECODE(SUBSTRB(C8.ORD_TEL_TMP,1,2),'02',SUBSTRB(C8.ORD_TEL_TMP,3,4),SUBSTRB(C8.ORD_TEL_TMP,4,3))")
			.append(" ,9,SUBSTRB(C8.ORD_TEL_TMP,3,3) ")
			.append(" ,8,SUBSTRB(C8.ORD_TEL_TMP,1,4) ")
			.append(" ,7,SUBSTRB(C8.ORD_TEL_TMP,1,3) ")
			.append(" ,'0000' ")
			.append(" ) ")
			.append(" ELSE ")
			.append(" '0000' ")
			.append(" END AS ORDRR_TEL_NO2 ")
			.append(" ,CASE WHEN LENGTH(C8.ORD_TEL_TMP) >= 7 AND LENGTH(C8.ORD_TEL_TMP) <=12 THEN DECODE(LENGTH(C8.ORD_TEL_TMP) ,12,SUBSTRB(C8.ORD_TEL_TMP,9,4) ")
			.append(" ,11,SUBSTRB(C8.ORD_TEL_TMP,8,4) ,10,SUBSTRB(C8.ORD_TEL_TMP,7,4) ,9,SUBSTRB(C8.ORD_TEL_TMP,6,4) ,8,SUBSTRB(C8.ORD_TEL_TMP,5,4) ,7,SUBSTRB(C8.ORD_TEL_TMP,4,4)")
			.append(" ,'0000') ELSE '0000' END AS ORDRR_TEL_NO3 ,CASE ")
			.append(" WHEN LENGTH(C8.ORD_HTEL_TMP) >= 7 AND LENGTH(C8.ORD_HTEL_TMP) <=12 THEN DECODE(LENGTH(C8.ORD_HTEL_TMP) ,12,SUBSTRB(C8.ORD_HTEL_TMP,1,4) ,11,SUBSTRB(C8.ORD_HTEL_TMP,1,3) ")
			.append(" ,10,DECODE(SUBSTRB(C8.ORD_HTEL_TMP,1,2),'02',SUBSTRB(C8.ORD_HTEL_TMP,1,2),SUBSTRB(C8.ORD_HTEL_TMP,1,3)) ")
			.append(" ,9,SUBSTRB(C8.ORD_HTEL_TMP,1,2) ,'00' ) ELSE '00' END AS ORDRR_CELL_NO1 ,CASE WHEN LENGTH(C8.ORD_HTEL_TMP) >= 7 AND LENGTH(C8.ORD_HTEL_TMP) <=12 THEN ")
			.append(" DECODE(LENGTH(C8.ORD_HTEL_TMP) ,12,SUBSTRB(C8.ORD_HTEL_TMP,5,4) ,11,SUBSTRB(C8.ORD_HTEL_TMP,4,4) ")
			.append(" ,10,DECODE(SUBSTRB(C8.ORD_HTEL_TMP,1,2),'02',SUBSTRB(C8.ORD_HTEL_TMP,3,4),SUBSTRB(C8.ORD_HTEL_TMP,4,3)) ,9,SUBSTRB(C8.ORD_HTEL_TMP,3,3) ,8,SUBSTRB(C8.ORD_HTEL_TMP,1,4) ")
			.append(" ,7,SUBSTRB(C8.ORD_HTEL_TMP,1,3),'0000') ELSE '0000' END AS ORDRR_CELL_NO2 ")
			.append(" ,CASE WHEN LENGTH(C8.ORD_HTEL_TMP) >= 7 AND LENGTH(C8.ORD_HTEL_TMP) <=12 THEN ")
			.append(" DECODE(LENGTH(C8.ORD_HTEL_TMP) ")
			.append(" ,12,SUBSTRB(C8.ORD_HTEL_TMP,9,4) ")
			.append(" ,11,SUBSTRB(C8.ORD_HTEL_TMP,8,4) ")
			.append(" ,10,SUBSTRB(C8.ORD_HTEL_TMP,7,4) ")
			.append(" ,9,SUBSTRB(C8.ORD_HTEL_TMP,6,4) ")
			.append(" ,8,SUBSTRB(C8.ORD_HTEL_TMP,5,4) ")
			.append(" ,7,SUBSTRB(C8.ORD_HTEL_TMP,4,4) ")
			.append(" ,'0000' ")
			.append(" ) ")
			.append(" ELSE ")
			.append(" '0000' ")
			.append(" END AS ORDRR_CELL_NO3 ")
			.append(" ,'' AS ORDRR_SAFE_NO1 ")
			.append(" ,'' AS ORDRR_SAFE_NO2 ")
			.append(" ,'' AS ORDRR_SAFE_NO3 ")
			.append(" ,NVL(DECODE(M1.REAL_DELIVERY_CD , 'Z999' ,M1.ORD_ZIP_CD1 ,C6.ZIP_CD1) ,' ') ")
			.append(" || NVL(DECODE(M1.REAL_DELIVERY_CD , 'Z999' ,M1.ORD_ZIP_CD2 ,C6.ZIP_CD2) ,' ') AS ORDRR_ZIP_NO ")
			.append(" ,REPLACE(NVL(DECODE(M1.REAL_DELIVERY_CD , 'Z999' ,M1.ORD_BASIC ,C6.ADDR_BASIC) ,' '),CHR(39),'') AS ORDRR_ADDR ")
			.append(" , REPLACE(NVL(DECODE(M1.REAL_DELIVERY_CD , 'Z999' ,M1.ORD_BASIC ,C6.ADDR_BASIC) ,' '),CHR(39),'') ||' '||")
			.append(" REPLACE(NVL(DECODE(M1.REAL_DELIVERY_CD , 'Z999' ,M1.ORD_DETAIL ,C6.ADDR_DETAIL) ,' '),CHR(39),'') AS ORDRR_DETAIL_ADDR ")
			.append(" ,NVL(M3.EXPRESS_NO ,' ') AS INVC_NO ")
			.append(" ,DECODE(C7.SUB_CD, 'D3' ,M3.EXPRESS_NO ,' ') AS ORI_INVC_NO ")
			.append(" ,'' AS ORI_ORD_NO ")
			.append(" ,'' AS COLCT_EXPCT_YMD ")
			.append(" ,'' AS COLCT_EXPCT_HOUR ,'' AS SHIP_EXPCT_YMD ")
			.append(" ,'' AS SHIP_EXPCT_HOUR ,DECODE(C7.SUB_CD, 'D3' ,'01', '02') AS PRT_ST ,'' AS ARTICLE_AMT ")
			.append(" ,NVL(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(M1.DELIVERY_MSG, CHR(13)||CHR(10) ,' '),'&NBSP;',' '),'&AMP;',''),'&LT;','')  ")
			.append(" ,'&GT;','') ,'&QUOT;','') ,' ') AS REMARK_1 ,'' AS REMARK_2 ,'' AS REMARK_3 ,'' AS COD_YN ,NVL(MAX(M2.ITEM_CD) ,' ') AS GDS_CD ")
			.append(" ,CASE WHEN COUNT(M2.ITEM_CD) = 1 THEN NVL(SUBSTRB(MAX(C1.ITEM_NM) ,1 ,40) ,' ') ")
			.append(" ELSE NVL(SUBSTRB( (SELECT ITEM_NM FROM CMITEM WHERE BRAND_CD=M1.BRAND_CD AND ITEM_CD=( SELECT MAX(ITEM_CD) FROM LO020ND WHERE CENTER_CD=M1.CENTER_CD AND BRAND_CD=M1.BRAND_CD ")
			.append(" AND OUTBOUND_DATE=M1.OUTBOUND_DATE AND OUTBOUND_NO=M1.OUTBOUND_NO )) ||' 외 '|| CAST(COUNT(M2.ITEM_CD)-1 AS VARCHAR2(10)) ||'건',1 ,40) , ' ' ) END AS GDS_NM ")
			.append(" ,1 AS GDS_QTY ,'' AS UNIT_CD ,'' AS UNIT_NM ,'' AS GDS_AMT ,'' AS ETC_1 ,'' AS ETC_2 ,'' AS ETC_3 ")
			.append(" ,'' AS ETC_4 ,'' AS ETC_5 ,'01' AS DLV_DV ,'N' AS RCPT_ERR_YN ,'' AS RCPT_ERR_MSG ,'01' AS EAI_PRGS_ST ,'' AS ")
			.append(" EAI_ERR_MSG ,'ISEC' AS REG_EMP_ID,'ISEC' AS MODI_EMP_ID ")
			.append(" FROM    LO020NM             M1 ")
			.append(" JOIN LO020ND        M2  ON  M2.CENTER_CD        = M1.CENTER_CD ")
			.append(" AND M2.BRAND_CD         = M1.BRAND_CD ")
			.append(" AND M2.OUTBOUND_DATE    = M1.OUTBOUND_DATE ")
			.append(" AND M2.OUTBOUND_NO      = M1.OUTBOUND_NO ")
			.append(" JOIN LO020NP        M3  ON  M3.CENTER_CD        = M2.CENTER_CD ")
			.append(" AND M3.BRAND_CD         = M2.BRAND_CD ")
			.append(" AND M3.OUTBOUND_DATE    = M2.OUTBOUND_DATE ")
			.append(" AND M3.OUTBOUND_NO      = M2.OUTBOUND_NO ")
			.append(" AND M3.LINE_NO          = M2.LINE_NO ")
			.append(" JOIN CMITEM         C1  ON  C1.BRAND_CD         = M2.BRAND_CD ")
			.append(" AND C1.ITEM_CD          = M2.ITEM_CD ")
			.append(" JOIN CMEXPRESSCONST C2  ON  C2.BRAND_CD         = M3.BRAND_CD ")
			.append(" AND C2.CENTER_CD       = M3.CENTER_CD ")
			.append(" AND C2.EXPRESS_CD       = M3.EXPRESS_CD ")
			.append(" JOIN CMBRAND        C3  ON  C3.BRAND_CD         = M1.BRAND_CD ")
			.append(" JOIN CMCUST         C4  ON  C4.CUST_CD          = C3.CUST_CD ")
			.append(" JOIN CMCENTER       C5  ON  C5.CENTER_CD        = M1.CENTER_CD ")
			.append(" JOIN CMDELIVERY     C6  ON  C6.BRAND_CD         = M1.BRAND_CD ")
			.append(" AND C6.DELIVERY_CD      = M1.REAL_DELIVERY_CD ")
			.append(" JOIN CMCODE         C7  ON  C7.CODE_CD          = M1.INOUT_CD ")
			.append(" AND C7.CODE_GRP         = 'LDIV03' ")
			.append(" AND C7.SUB_CD           IN ('D1','D2','D3') ")
			.append(" JOIN    ( ")
			.append(" SELECT  ")
			.append(" A.CENTER_CD " )
			.append(" ,A.BRAND_CD ")
			.append(" ,A.OUTBOUND_DATE ")
			.append(" ,A.OUTBOUND_NO ")
			.append(" ,B.LINE_NO ")
			.append(" ,CASE ")
			.append(" WHEN DECODE(A.REAL_DELIVERY_CD , 'Z999' ,A.ACPER_TEL ,D.TEL_NO) IS NULL THEN ")
			.append(" '0' ")
			.append(" ELSE ")
			.append(" TRANSLATE(DECODE(A.REAL_DELIVERY_CD , 'Z999' ,A.ACPER_TEL ,D.TEL_NO),'0123456789-)','0123456789') ")
			.append(" END    AS ACPER_TEL_TMP ")
			.append(" ,CASE ")
			.append(" WHEN DECODE(A.REAL_DELIVERY_CD , 'Z999' ,A.ACPER_HP ,D.CHARGE_TEL) IS NULL THEN ")
			.append(" '0' ")
			.append(" ELSE ")
			.append(" TRANSLATE(DECODE(A.REAL_DELIVERY_CD , 'Z999' ,A.ACPER_HP ,D.CHARGE_TEL),'0123456789-)','0123456789') ")
			.append(" END    AS ACPER_HTEL_TMP ")
			.append(" ,CASE ")
			.append(" WHEN DECODE(A.REAL_DELIVERY_CD , 'Z999' ,A.ORD_TEL ,D.TEL_NO) IS NULL THEN ")
			.append(" '0' ")
			.append(" ELSE ")
			.append(" TRANSLATE(DECODE(A.REAL_DELIVERY_CD , 'Z999' ,A.ORD_TEL ,D.TEL_NO),'0123456789-)','0123456789') ")
			.append(" END    AS ORD_TEL_TMP ")
			.append(" ,CASE ")
			.append(" WHEN DECODE(A.REAL_DELIVERY_CD , 'Z999' ,A.ORD_HP ,D.CHARGE_TEL) IS NULL THEN ")
			.append(" '0' ")
			.append(" ELSE ")
			.append(" TRANSLATE(DECODE(A.REAL_DELIVERY_CD , 'Z999' ,A.ORD_HP ,D.CHARGE_TEL),'0123456789-)','0123456789') ")
			.append(" END    AS ORD_HTEL_TMP ")
			.append(" ,CASE ")
			.append(" WHEN D.FREE_VAL2 IS NULL THEN ")
			.append(" '0' ")
			.append(" ELSE ")
			.append(" TRANSLATE(D.FREE_VAL2,'0123456789-)','0123456789') ")
			.append(" END    AS CENTER_TEL_TMP ")
			.append(" FROM    LO020NM             A ")
			.append(" JOIN LO020ND        B   ON  B.CENTER_CD         = A.CENTER_CD ")
			.append(" AND B.BRAND_CD          = A.BRAND_CD ")
			.append(" AND B.OUTBOUND_DATE     = A.OUTBOUND_DATE ")
			.append(" AND B.OUTBOUND_NO       = A.OUTBOUND_NO ")
			.append(" JOIN LO020NP        C   ON  C.CENTER_CD         = B.CENTER_CD ")
			.append(" AND C.BRAND_CD          = B.BRAND_CD ")
			.append(" AND C.OUTBOUND_DATE     = B.OUTBOUND_DATE ")
			.append(" AND C.OUTBOUND_NO       = B.OUTBOUND_NO ")
			.append(" AND C.LINE_NO           = B.LINE_NO ")
			.append(" LEFT OUTER JOIN CMDELIVERY     D   ON  D.BRAND_CD          = A.BRAND_CD ")
			.append(" AND D.DELIVERY_CD       = DECODE(A.DELIVERY_CD, 'Z999', A.FREE_VAL3, A.DELIVERY_CD) ")
			.append(" JOIN CMEXPRESSCONST E   ON  E.BRAND_CD          = C.BRAND_CD ")
			.append(" AND E.CENTER_CD        = C.CENTER_CD ")
			.append(" AND E.EXPRESS_CD        = C.EXPRESS_CD ")
			.append(" JOIN CMCENTER       F   ON  F.CENTER_CD         = A.CENTER_CD  ")
			.append(" WHERE   A.CENTER_CD    LIKE '%' ")
			.append(" AND     A.BRAND_CD     LIKE '%' ")
			.append(" AND     A.OUTBOUND_DATE <= TRUNC(SYSDATE) ")
			.append(" AND     B.OUTBOUND_STATE = '50' ")
			.append(" AND     B.CONFIRM_QTY > 0 ")
			.append(" AND     E.EXPRESS_CD = '09' ")
			.append(" )   C8  ON  C8.CENTER_CD        = M1.CENTER_CD ")
			.append(" AND C8.BRAND_CD         = M1.BRAND_CD ")
			.append(" AND C8.OUTBOUND_DATE    = M1.OUTBOUND_DATE ")
			.append(" AND C8.OUTBOUND_NO      = M1.OUTBOUND_NO ")
			.append(" AND C8.LINE_NO          = M2.LINE_NO ")
			.append(" WHERE   M1.CENTER_CD    LIKE '%' ")
			.append(" AND     M1.BRAND_CD     LIKE '%' ")
			.append(" AND     M1.OUTBOUND_DATE <= TRUNC(SYSDATE) ")
			.append(" AND     M2.OUTBOUND_STATE = '50' ")
			.append(" AND     M2.CONFIRM_QTY > 0 ")
			.append(" AND     M1.FREE_VAL5 = '10' ")
			.append(" AND     C2.EXPRESS_CD = '09' ")
			.append(" GROUP BY  ")
			.append(" M1.CENTER_CD ")
			.append(" ,M1.BRAND_CD ")
			.append(" ,M1.OUTBOUND_DATE ")
			.append(" ,M1.OUTBOUND_NO ")
			.append(" ,M1.REAL_DELIVERY_CD ")
			.append(" ,M1.ACPER_NM ")
			.append(" ,M1.ACPER_ZIP_CD1 ")
			.append(" ,M1.ACPER_ZIP_CD2 ")
			.append(" ,M1.ACPER_BASIC ")
			.append(" ,M1.ACPER_DETAIL ")
			.append(" ,M1.ORD_NM ")
			.append(" ,M1.ORD_ZIP_CD1 ")
			.append(" ,M1.ORD_ZIP_CD2 ")
			.append(" ,M1.ORD_BASIC ")
			.append(" ,M1.ORD_DETAIL ")
			.append(" ,M1.DELIVERY_MSG ")
			.append(" ,M1.BOX_NO ")
			.append(" ,M2.BRAND_NO ")
			.append(" ,M3.EXPRESS_NO ")
			.append(" ,M3.BOX_NO ")
			.append(" ,C2.CUSTOMER_CD ")
			.append(" ,C2.JOINT_CD ")
			.append(" ,C2.JOINT_NM ")
			.append(" ,C5.CENTER_NM ")
			.append(" ,C5.TEL_NO ")
			.append(" ,C5.ZIP_CD1 ")
			.append(" ,C5.ZIP_CD2 ")
			.append(" ,C5.ADDR_BASIC ")
			.append(" ,C5.ADDR_DETAIL ")
			.append(" ,C6.DELIVERY_NM ")
			.append(" ,C6.ZIP_CD1 ")
			.append(" ,C6.ZIP_CD2 ")
			.append(" ,C6.ADDR_BASIC ")
			.append(" ,C6.ADDR_DETAIL ")
			.append(" ,C7.SUB_CD ")
			.append(" ,C8.ACPER_TEL_TMP ")
			.append(" ,C8.ACPER_HTEL_TMP ")
			.append(" ,C8.ORD_TEL_TMP ")
			.append(" ,C8.ORD_HTEL_TMP ")
			.append(" ,C8.CENTER_TEL_TMP ");
			
			//.append(" ) "); 
			
			logger.info(sb.toString());
			pstmt = conn.prepareStatement(sb.toString());
			rs = pstmt.executeQuery();
			rsmd = rs.getMetaData();
			int colCnt = rsmd.getColumnCount();
			while (rs.next()) {
				String[] row = new String[colCnt];
				for(int i=1; i<=colCnt; i++) {
					row[i-1] = rs.getString(i);
				}
				arrLoData.add(row);
			}
			
			if(arrLoData.size() > 0) {
				logger.info("selectLoData :: Success !");
			} else {
				logger.info("selectLoData :: No data !");
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
		logger.info("selectLoData :: End !");
		return arrLoData;
	}
	
	public int insertLoData(Connection conn, ArrayList<String[]> arrLoData) {
		logger.info("insertLoData :: Start !");
		StringBuilder sb= new StringBuilder();
		 int result = -1;
		 try {
			 sb.append(" insert into EDIEXPRESS_CJ_SEND values ( ?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?," +
				 		"?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?," +
				 		"?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?," +
				 		"?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?," +
				 		"?,?,?,?,SYSDATE,?,SYSDATE)" );
			 this.pstmt = conn.prepareStatement(sb.toString());	
			 for(int i=0; i<arrLoData.size(); i++) {
				 for(int j=0; j<arrLoData.get(i).length; j++) {
					 pstmt.setString(j+1, arrLoData.get(i)[j]);
				 }
				 result = pstmt.executeUpdate();
				 if(result < 1) {
					 break;
				 }
			 }
		 } catch (Exception e) {
			 e.printStackTrace();
			 try {
				 result = -1;
				 conn.rollback();
			 } catch (Exception e1) {
				 result = -1;
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
		 logger.info("insertLoData :: End !");
		 return result;
	}
		
	public int runCJ(Connection connCJ, ArrayList<String[]> arrLoData){
		StringBuilder sb = new StringBuilder();
		int result = -1;
		try {
			logger.info("run_CJ :: Start !");
			String sendrDetailAddr = "";
			String rcvrDetailAddr = "";
			String ordrrDetailAddr = "";
			pstmt = connCJ.prepareStatement(sb.toString());
			for(int i=0; i<arrLoData.size(); i++) {
				
				sendrDetailAddr = arrLoData.get(i)[29];
				rcvrDetailAddr = arrLoData.get(i)[42];			
				ordrrDetailAddr = arrLoData.get(i)[55];
				sb.append(" SELECT CASE WHEN LENGTHB('").append(sendrDetailAddr).append("') > 50 THEN ")
				  .append(" FC_KX_ADDR_SEPARATION('ADDR_1', '").append(sendrDetailAddr).append("')")
				  .append(" ELSE '").append(sendrDetailAddr).append("'")
				  .append(" END AS SENDR_ADDR ")
				  .append(" , CASE WHEN LENGTHB('").append(sendrDetailAddr).append("') > 50 THEN ")
				  .append(" FC_KX_ADDR_SEPARATION('ADDR_2', '").append(sendrDetailAddr).append("')")
				  .append(" ELSE '.' ")
				  .append(" END AS SENDR_DETAIL_ADDR ")
				  .append(" , CASE WHEN LENGTHB('").append(rcvrDetailAddr).append("') > 50 THEN ")
				  .append(" FC_KX_ADDR_SEPARATION('ADDR_1', '").append(rcvrDetailAddr).append("')")
				  .append(" ELSE '").append(rcvrDetailAddr).append("'")
				  .append(" END AS RCVR_ADDR ")
				  .append(" , CASE WHEN LENGTHB('").append(rcvrDetailAddr).append("') > 50 THEN ")
				  .append(" FC_KX_ADDR_SEPARATION('ADDR_2', '").append(rcvrDetailAddr).append("')")
				  .append(" ELSE '.'")
				  .append(" END AS RCVR_DETAIL_ADDR ")
				  .append(" , CASE WHEN LENGTHB('").append(ordrrDetailAddr).append("') > 50 THEN ")
				  .append(" FC_KX_ADDR_SEPARATION('ADDR_1', '").append(ordrrDetailAddr).append("')")
				  .append(" ELSE '").append(ordrrDetailAddr).append("'")
				  .append(" END AS ORDRR_ADDR ")
				  .append(" , CASE WHEN LENGTHB('").append(ordrrDetailAddr).append("') > 50 THEN ")
				  .append(" FC_KX_ADDR_SEPARATION('ADDR_2', '").append(ordrrDetailAddr).append("')")
				  .append(" ELSE '.'")
				  .append(" END AS ORDRR_DETAIL_ADDR ")	 
				  .append(" FROM DUAL ");
				
				
				logger.info(sb.toString());
				rs = pstmt.executeQuery(sb.toString());
				String rsSendrAddr = "";
				String rsSendrDetailAddr = "";
				String rsRcvrAddr = "";
				String rsRcvrDetailAddr = "";
				String rsOrdrrAddr = "";
				String rsOrdrrDetailAddr = "";
				
				while(rs.next()) {
					rsSendrAddr = rs.getString("SENDR_ADDR");
					rsSendrDetailAddr = rs.getString("SENDR_DETAIL_ADDR");
					rsRcvrAddr = rs.getString("RCVR_ADDR");
					rsRcvrDetailAddr = rs.getString("RCVR_DETAIL_ADDR");
					rsOrdrrAddr = rs.getString("ORDRR_ADDR");
					rsOrdrrDetailAddr = rs.getString("ORDRR_DETAIL_ADDR");
					
					if(rsSendrAddr == null) {
						rsSendrAddr = rsSendrDetailAddr;
						rsSendrDetailAddr = ".";
					}
					
					if(rsRcvrAddr == null){
						rsRcvrAddr = rsRcvrDetailAddr;
						rsRcvrDetailAddr = ".";
					}
					
					if(rsOrdrrAddr == null){
						rsOrdrrAddr = rsOrdrrDetailAddr;
						rsOrdrrDetailAddr = ".";
					}
					
					arrLoData.get(i)[28] = rsSendrAddr;
					arrLoData.get(i)[29] = rsSendrDetailAddr;
					arrLoData.get(i)[41] = rsRcvrAddr;
					arrLoData.get(i)[42] = rsRcvrDetailAddr;
					arrLoData.get(i)[54] = rsOrdrrAddr;
					arrLoData.get(i)[55] = rsOrdrrDetailAddr;
					break;	
				}
				sb.delete(0, sb.length());
			}
			
			result = insertCJ(connCJ, arrLoData);
		} catch (Exception e) {
			loggerErr.error(e.getMessage());
			try {
				connCJ.rollback();
			} catch (Exception e1) {
				loggerErr.error(e.getMessage());
			}
		} finally {
			try {
				close();
			} catch (Exception e) {
				loggerErr.error(e.getMessage());
			}
		}
		logger.info("run_cj :: End");
		return result;
	}
	
	private int insertCJ(Connection conn, ArrayList<String[]> arrLoData) throws Exception {
		logger.info("insertCJ :: Start");
		 StringBuilder sb= new StringBuilder();
		 int result = -1;
		 try {
			 sb.append(" insert into V_RCPT_ISEC010 values ( ?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?," +
				 		"?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?," +
				 		"?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?," +
				 		"?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?," +
				 		"?,?,?,?,SYSDATE,?,SYSDATE)" );
			 this.pstmt = conn.prepareStatement(sb.toString());
			 for(int i=0; i<arrLoData.size(); i++) {
				 for(int j=2; j<arrLoData.get(i).length; j++) {
					 pstmt.setString((j-1), arrLoData.get(i)[j]);		
					 logger.debug("-------- : " + arrLoData.get(i)[j]);
				 }
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
				 result = -1;
				 loggerErr.error(e1.getMessage());
			 } 
			 loggerErr.error(e.getMessage());
		 }
		 
		 logger.info("insertCJ :: End");
		 return result;
	 }
	
	 public int insertSMS(Connection conn, String errMsg) {
 		int result = -1;
 		StringBuilder sb = new StringBuilder();
 		ArrayList<String> emergencyContacts = new ArrayList<String>(); //비상연락처
 		emergencyContacts.add("01023783091"); //이동신 팀장
 		emergencyContacts.add("01052760852"); //표성만 과장
 		emergencyContacts.add("01022462042"); //박정화
 		
 		if(errMsg.indexOf(":", 1) > 0) {
 			errMsg = errMsg.substring(0, errMsg.indexOf(":", 1));
 		}
 		
 		try {
 			pstmt = conn.prepareStatement(sb.toString());
	 		for(int i=0; i<emergencyContacts.size(); i++) {
	 			sb.append(" INSERT /* Order.orderConfirmSMS */ INTO CMSMSG@WIZWID ( ")
	 			  .append(" SEQNUM, ORDER_ID, SMSMSG_GB, DESTIN, SM, CALLBACK, REG_ID, ")
	              .append(" REG_DT, UPD_ID, UPD_DT) ")
	              .append(" VALUES ( ")
	              .append(" CMSMSG_SEQ.nextval@wizwid, ")
	              .append(" '','A0', '").append(emergencyContacts.get(i)).append("' ,")
	              .append(" '").append(errMsg).append("', '07074252151','000000101',")
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
