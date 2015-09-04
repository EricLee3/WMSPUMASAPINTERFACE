package com.gics.edi;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.util.ArrayList;

import org.apache.log4j.Logger;

public class CJEDI_SEND_RT {
	private Logger logger  = Logger.getLogger("process.lr");
	private Logger loggerErr = Logger.getLogger("process.err");
 	private PreparedStatement pstmt;
 	private ResultSet rs;
	
	public int updateTarget(Connection conn) {
		int result = -1;
		StringBuilder sb = new StringBuilder();
		logger.info("updateTarget :: Start");
		try {
			sb.append(" UPDATE  LO010NM ")
			  .append(" SET FREE_VAL7 = '10' ")
			  .append(" WHERE   (CENTER_CD,BRAND_CD,ORDER_DATE,ORDER_NO) ")
			  .append(" IN ")
			  .append(" ( SELECT  DISTINCT M1.CENTER_CD ,M1.BRAND_CD ,M1.ORDER_DATE ")
			  .append(" ,M1.ORDER_NO FROM    LO010NM                     M1")
			  .append(" JOIN        LO010ND         M2  ON  M2.CENTER_CD    = M1.CENTER_CD ")
			  .append(" AND M2.BRAND_CD     = M1.BRAND_CD ")
			  .append(" AND M2.ORDER_DATE   = M1.ORDER_DATE AND M2.ORDER_NO     = M1.ORDER_NO ")
			  .append(" LEFT OUTER JOIN LO020NM     M3  ON  M3.CENTER_CD    = M1.CENTER_CD ")
			  .append(" AND M3.BRAND_CD     = M1.BRAND_CD AND M3.ORDER_DATE   = M1.ORDER_DATE ")
			  .append(" AND M3.ORDER_NO     = M1.ORDER_NO JOIN        CMDELIVERY      C1  ON  C1.BRAND_CD     = M1.BRAND_CD ")
			  .append(" AND C1.DELIVERY_CD  = M1.DELIVERY_CD JOIN        CMDELIVERY      C2  ON  C2.BRAND_CD     = M1.BRAND_CD ")
			  .append(" AND C2.DELIVERY_CD  = M1.REAL_DELIVERY_CD JOIN        CMCODE          C3  ON  C3.CODE_CD      = M1.INOUT_CD ")
			  .append(" AND C3.CODE_GRP     = 'LDIV03' ")
			  .append(" AND C3.SUB_CD       IN ('E3') ")
			  .append(" JOIN        CMITEM          C4  ON  C4.BRAND_CD     = M2.BRAND_CD ")
			  .append(" AND C4.ITEM_CD      = M2.ITEM_CD ")
			  .append(" JOIN        CMEXPRESSCONST  C5  ON  C5.BRAND_CD     = M1.BRAND_CD AND M1.CENTER_CD = C5.CENTER_CD AND C5.EXPRESS_CD   = '09'  ")
			  .append(" JOIN        CMBRAND         C6  ON  C6.BRAND_CD     = M1.BRAND_CD ")
			  .append(" JOIN        CMCUST          C7  ON  C7.CUST_CD      = C6.CUST_CD ")
			  .append(" JOIN        CMCENTER        C8  ON  C8.CENTER_CD    = M1.CENTER_CD ")
			  .append(" JOIN        CMDELIVERY      C9  ON  C9.BRAND_CD     = M1.BRAND_CD ")
			  .append(" AND C9.DELIVERY_CD  = M1.FREE_VAL3 WHERE   M1.CENTER_CD        LIKE '%' ")
			  .append(" AND     M1.BRAND_CD         LIKE '%'")
			  .append(" AND     M1.ORDER_DATE       BETWEEN TO_DATE('20130325','YYYYMMDD') AND TRUNC(SYSDATE) ")
			  .append(" AND     M1.OUTBOUND_STATE   < '20' AND     (M1.FREE_VAL7       = '00' OR M1.FREE_VAL7 IS NULL)")
			  .append(" AND     M1.PICKUP_YN        = 'Y'")
			  .append(" AND     NOT EXISTS          (SELECT CODE_CD FROM CMCODE WHERE CODE_GRP='LDIV25' AND CODE_CD=M1.FREE_VAL6)")
			  .append(" AND     M3.OUTBOUND_STATE   IS NULL ) ");
			 pstmt = conn.prepareStatement(sb.toString());
			 result = pstmt.executeUpdate();
			 
			 if(result>0) {
				 logger.info(" updateTarget :: Success !");
			 } else {
				 logger.info(" updateTarget :: No data !");
			 }
		} catch (Exception e) {
			try {
				conn.rollback();
			} catch (Exception e1) {
				loggerErr.error(e1.getMessage());
			}
			e.printStackTrace();
			logger.info(e.getMessage());
		} finally {
			try {
				close();
			} catch (Exception e) {
				loggerErr.error(e.getMessage());
			}
		}
		logger.info("updateTarget :: End");
		return result;
	}
	
	public ArrayList<String[]> selectLrData(Connection conn) {
		logger.info("selectLrData :: Start");
		StringBuilder sb = new StringBuilder();
		ArrayList<String[]> arrLrData = new ArrayList<String[]>();
		ResultSetMetaData rsmd =  null;
		try {
//			sb.append(" INSERT INTO EDIEXPRESS_CJ_SEND ")
			 sb.append(" SELECT M1.CENTER_CD ")
			 .append(" ,M1.BRAND_CD ,NVL(C5.CUSTOMER_CD ,' ') AS CUST_ID ,TO_CHAR(TO_DATE(SYSDATE) ,'YYYYMMDD') AS RCPT_YMD ")
			 .append(" ,M1.CENTER_CD||M1.BRAND_CD||TO_CHAR(M1.ORDER_DATE,'YYYYMMDD')||M1.ORDER_NO AS CUST_USE_NO ")
			 .append(" ,'02' AS RCPT_DV,'01' AS WORK_DV_CD ")
			 .append(" ,'01' AS REQ_DV_CD ,TO_CHAR(TO_DATE(SYSDATE),'YYYYMMDD') ||'_'|| C5.CUSTOMER_CD ||'_'|| M1.BRAND_CD || M2.BRAND_NO AS MPCK_KEY ")
			 .append(" ,1 AS MPCK_SEQ ,'01' AS CAL_DV_CD ,'03' AS FRT_DV_CD ,'01' AS CNTR_ITEM_CD ,'01' AS BOX_TYPE_CD ,1 AS BOX_QTY ,0 AS FRT ,NVL(C5.JOINT_CD ,' ') AS CUST_MGMT_DLCM_CD ")
			 .append(" ,NVL(M1.ACPER_NM ,' ') AS SENDR_NM ,CASE WHEN LENGTH(C9.ACPER_TEL_TMP) >= 7 AND LENGTH(C9.ACPER_TEL_TMP) <=12 THEN ")
			 .append(" DECODE(LENGTH(C9.ACPER_TEL_TMP) ,12,SUBSTR(C9.ACPER_TEL_TMP,1,4) ,11,SUBSTR(C9.ACPER_TEL_TMP,1,3) ")
			 .append(" ,10,DECODE(SUBSTR(C9.ACPER_TEL_TMP,1,2),'02',SUBSTR(C9.ACPER_TEL_TMP,1,2),SUBSTR(C9.ACPER_TEL_TMP,1,3)) ")
			 .append(" ,9,SUBSTR(C9.ACPER_TEL_TMP,1,2),'00')")
			 .append(" ELSE '00' END AS SENDR_TEL_NO1 ,CASE WHEN LENGTH(C9.ACPER_TEL_TMP) >= 7 AND LENGTH(C9.ACPER_TEL_TMP) <=12 THEN ")
			 .append(" DECODE(LENGTH(C9.ACPER_TEL_TMP) ,12,SUBSTR(C9.ACPER_TEL_TMP,5,4) ,11,SUBSTR(C9.ACPER_TEL_TMP,4,4) ")
			 .append(" ,10,DECODE(SUBSTR(C9.ACPER_TEL_TMP,1,2),'02',SUBSTR(C9.ACPER_TEL_TMP,3,4),SUBSTR(C9.ACPER_TEL_TMP,4,3)) ")
			 .append(" ,9,SUBSTR(C9.ACPER_TEL_TMP,3,3),8,SUBSTR(C9.ACPER_TEL_TMP,1,4),7,SUBSTR(C9.ACPER_TEL_TMP,1,3),'0000' ")
			 .append(") ELSE '0000' END AS SENDR_TEL_NO2 ,CASE WHEN LENGTH(C9.ACPER_TEL_TMP) >= 7 AND LENGTH(C9.ACPER_TEL_TMP) <=12 THEN ")
			 .append(" DECODE(LENGTH(C9.ACPER_TEL_TMP) ,12,SUBSTR(C9.ACPER_TEL_TMP,9,4) ,11,SUBSTR(C9.ACPER_TEL_TMP,8,4) ,10,SUBSTR(C9.ACPER_TEL_TMP,7,4)")
			 .append(" ,9,SUBSTR(C9.ACPER_TEL_TMP,6,4) ,8,SUBSTR(C9.ACPER_TEL_TMP,5,4) ,7,SUBSTR(C9.ACPER_TEL_TMP,4,4),'0000') ELSE ")
			 .append(" '0000' END AS SENDR_TEL_NO3 ,CASE WHEN LENGTH(C9.ACPER_HTEL_TMP) >= 7 AND LENGTH(C9.ACPER_HTEL_TMP) <=12 THEN ")
			 .append(" DECODE(LENGTH(C9.ACPER_HTEL_TMP) ,12,SUBSTR(C9.ACPER_HTEL_TMP,1,4) ,11,SUBSTR(C9.ACPER_HTEL_TMP,1,3) ")
			 .append(" ,10,DECODE(SUBSTR(C9.ACPER_HTEL_TMP,1,2),'02',SUBSTR(C9.ACPER_HTEL_TMP,1,2),SUBSTR(C9.ACPER_HTEL_TMP,1,3)) ")
			 .append(" ,9,SUBSTR(C9.ACPER_HTEL_TMP,1,2),'00') ELSE '00' END AS SENDR_CELL_NO1 ,CASE WHEN LENGTH(C9.ACPER_HTEL_TMP) >= 7 AND LENGTH(C9.ACPER_HTEL_TMP) <=12 THEN")
			 .append(" DECODE(LENGTH(C9.ACPER_HTEL_TMP) ,12,SUBSTR(C9.ACPER_HTEL_TMP,5,4) ,11,SUBSTR(C9.ACPER_HTEL_TMP,4,4) ")
			 .append(" ,10,DECODE(SUBSTR(C9.ACPER_HTEL_TMP,1,2),'02',SUBSTR(C9.ACPER_HTEL_TMP,3,4),SUBSTR(C9.ACPER_HTEL_TMP,4,3)) ")
			 .append(" ,9,SUBSTR(C9.ACPER_HTEL_TMP,3,3),8,SUBSTR(C9.ACPER_HTEL_TMP,1,4),7,SUBSTR(C9.ACPER_HTEL_TMP,1,3),'0000') ELSE '0000' END AS SENDR_CELL_NO2")
			 .append(" ,CASE WHEN LENGTH(C9.ACPER_HTEL_TMP) >= 7 AND LENGTH(C9.ACPER_HTEL_TMP) <=12 THEN DECODE(LENGTH(C9.ACPER_HTEL_TMP) ,12,SUBSTR(C9.ACPER_HTEL_TMP,9,4)")
			 .append(" ,11,SUBSTR(C9.ACPER_HTEL_TMP,8,4) ,10,SUBSTR(C9.ACPER_HTEL_TMP,7,4) ,9,SUBSTR(C9.ACPER_HTEL_TMP,6,4) ,8,SUBSTR(C9.ACPER_HTEL_TMP,5,4),7,SUBSTR(C9.ACPER_HTEL_TMP,4,4)")
			 .append(" ,'0000') ELSE '0000' END AS SENDR_CELL_NO3,'' AS SENDR_SAFE_NO1,'' AS SENDR_SAFE_NO2,'' AS SENDR_SAFE_NO3,NVL(M1.ACPER_ZIP_CD1 ,' ') || NVL(M1.ACPER_ZIP_CD2 ,' ') AS SENDR_ZIP_NO")
			 .append(" ,REPLACE(NVL(M1.ACPER_BASIC ,' '), CHR(39), '') AS SENDR_ADDR ,REPLACE(NVL(M1.ACPER_BASIC ,' '),CHR(39),'') ||' '|| REPLACE(NVL(M1.ACPER_DETAIL ,'.'),CHR(39), '') AS SENDR_DETAIL_ADDR")
			 .append(" ,NVL(C8.CENTER_NM ,' ') AS RCVR_NM ,CASE WHEN LENGTH(C9.CENTER_TEL_TMP) >= 7 AND LENGTH(C9.CENTER_TEL_TMP) <=12 THEN ")
			 .append(" DECODE(LENGTH(C9.CENTER_TEL_TMP) ,12,SUBSTR(C9.CENTER_TEL_TMP,1,4) ,11,SUBSTR(C9.CENTER_TEL_TMP,1,3) ")
			 .append(" ,10,DECODE(SUBSTR(C9.CENTER_TEL_TMP,1,2),'02',SUBSTR(C9.CENTER_TEL_TMP,1,2),SUBSTR(C9.CENTER_TEL_TMP,1,3)) ,9,SUBSTR(C9.CENTER_TEL_TMP,1,2)")
			 .append(" ,'00') ELSE '00' END AS RCVR_TEL_NO1 ,CASE WHEN LENGTH(C9.CENTER_TEL_TMP) >= 7 AND LENGTH(C9.CENTER_TEL_TMP) <=12 THEN ")
			 .append(" DECODE(LENGTH(C9.CENTER_TEL_TMP) ,12,SUBSTR(C9.CENTER_TEL_TMP,5,4) ,11,SUBSTR(C9.CENTER_TEL_TMP,4,4) ")
			 .append(" ,10,DECODE(SUBSTR(C9.CENTER_TEL_TMP,1,2),'02',SUBSTR(C9.CENTER_TEL_TMP,3,4),SUBSTR(C9.CENTER_TEL_TMP,4,3)) ")
			 .append(" ,9,SUBSTR(C9.CENTER_TEL_TMP,3,3) ,8,SUBSTR(C9.CENTER_TEL_TMP,1,4) ,7,SUBSTR(C9.CENTER_TEL_TMP,1,3) ,'0000') ELSE '0000' END AS RCVR_TEL_NO2")
			 .append(" ,CASE WHEN LENGTH(C9.CENTER_TEL_TMP) >= 7 AND LENGTH(C9.CENTER_TEL_TMP) <=12 THEN DECODE(LENGTH(C9.CENTER_TEL_TMP) ")
			 .append(" ,12,SUBSTR(C9.CENTER_TEL_TMP,9,4) ,11,SUBSTR(C9.CENTER_TEL_TMP,8,4),10,SUBSTR(C9.CENTER_TEL_TMP,7,4),9,SUBSTR(C9.CENTER_TEL_TMP,6,4) ")
			 .append(" ,8,SUBSTR(C9.CENTER_TEL_TMP,5,4) ,7,SUBSTR(C9.CENTER_TEL_TMP,4,4),'0000') ELSE '0000' END AS RCVR_TEL_NO3 ")
			 .append(" ,'' AS RCVR_CELL_NO1 ,'' AS RCVR_CELL_NO2 ,'' AS RCVR_CELL_NO3 ,'' AS RCVR_SAFE_NO1 ,'' AS RCVR_SAFE_NO2 ,'' AS RCVR_SAFE_NO3 ")
			 .append(" ,NVL(C8.ZIP_CD1 ,' ') || NVL(C8.ZIP_CD2 ,' ') AS RCVR_ZIP_NO ,")
			 .append(" REPLACE(NVL(C8.ADDR_BASIC ,' '),CHR(39), '') AS RCVR_ADDR ")
			 .append(" ,REPLACE(NVL(C8.ADDR_BASIC ,' '),CHR(39), '') ||' '|| REPLACE(NVL(C8.ADDR_DETAIL ,'.'),CHR(39),'') AS RCVR_DETAIL_ADDR ,NVL(M1.ORD_NM ,' ') AS ORDRR_NM ,CASE WHEN LENGTH(C9.ORD_TEL_TMP) >= 7 AND LENGTH(C9.ORD_TEL_TMP) <=12 THEN ")
			 .append(" DECODE(LENGTH(C9.ORD_TEL_TMP) ,12,SUBSTR(C9.ORD_TEL_TMP,1,4) ,11,SUBSTR(C9.ORD_TEL_TMP,1,3) ")
			 .append(" ,10,DECODE(SUBSTR(C9.ORD_TEL_TMP,1,2),'02',SUBSTR(C9.ORD_TEL_TMP,1,2),SUBSTR(C9.ORD_TEL_TMP,1,3)),9,SUBSTR(C9.ORD_TEL_TMP,1,2) ,'00') ELSE '00' END AS ORDRR_TEL_NO1")
			 .append(" ,CASE WHEN LENGTH(C9.ORD_TEL_TMP) >= 7 AND LENGTH(C9.ORD_TEL_TMP) <=12 THEN DECODE(LENGTH(C9.ORD_TEL_TMP) ,12,SUBSTR(C9.ORD_TEL_TMP,5,4) ")
			 .append(" ,11,SUBSTR(C9.ORD_TEL_TMP,4,4) ,10,DECODE(SUBSTR(C9.ORD_TEL_TMP,1,2),'02',SUBSTR(C9.ORD_TEL_TMP,3,4),SUBSTR(C9.ORD_TEL_TMP,4,3)) ")
			 .append(" ,9,SUBSTR(C9.ORD_TEL_TMP,3,3) ,8,SUBSTR(C9.ORD_TEL_TMP,1,4) ,7,SUBSTR(C9.ORD_TEL_TMP,1,3) ,'0000' ) ELSE '0000' END AS ORDRR_TEL_NO2 ")
			 .append(" ,CASE WHEN LENGTH(C9.ORD_TEL_TMP) >= 7 AND LENGTH(C9.ORD_TEL_TMP) <=12 THEN DECODE(LENGTH(C9.ORD_TEL_TMP) ")
			 .append(" ,12,SUBSTR(C9.ORD_TEL_TMP,9,4) ,11,SUBSTR(C9.ORD_TEL_TMP,8,4) ,10,SUBSTR(C9.ORD_TEL_TMP,7,4) ,9,SUBSTR(C9.ORD_TEL_TMP,6,4) ,8,SUBSTR(C9.ORD_TEL_TMP,5,4) ")
			 .append(" ,7,SUBSTR(C9.ORD_TEL_TMP,4,4) ,'0000' ) ELSE '0000' END AS ORDRR_TEL_NO3 ,CASE WHEN LENGTH(C9.ORD_HTEL_TMP) >= 7 AND LENGTH(C9.ORD_HTEL_TMP) <=12 THEN ")
			 .append(" DECODE(LENGTH(C9.ORD_HTEL_TMP),12,SUBSTR(C9.ORD_HTEL_TMP,1,4) ")
			 .append(" ,11,SUBSTR(C9.ORD_HTEL_TMP,1,3),10,DECODE(SUBSTR(C9.ORD_HTEL_TMP,1,2),'02',SUBSTR(C9.ORD_HTEL_TMP,1,2),SUBSTR(C9.ORD_HTEL_TMP,1,3)),9,SUBSTR(C9.ORD_HTEL_TMP,1,2) ")
			 .append(" ,'00' ) ELSE '00' END AS ORDRR_CELL_NO1 ,CASE WHEN LENGTH(C9.ORD_HTEL_TMP) >= 7 AND LENGTH(C9.ORD_HTEL_TMP) <=12 THEN ")
			 .append(" DECODE(LENGTH(C9.ORD_HTEL_TMP) ,12,SUBSTR(C9.ORD_HTEL_TMP,5,4) ,11,SUBSTR(C9.ORD_HTEL_TMP,4,4) ") 
			 .append(" ,10,DECODE(SUBSTR(C9.ORD_HTEL_TMP,1,2),'02',SUBSTR(C9.ORD_HTEL_TMP,3,4),SUBSTR(C9.ORD_HTEL_TMP,4,3)) ,9,SUBSTR(C9.ORD_HTEL_TMP,3,3) ")
			 .append(" ,8,SUBSTR(C9.ORD_HTEL_TMP,1,4),7,SUBSTR(C9.ORD_HTEL_TMP,1,3),'0000') ELSE '0000' ")
			 .append(" END AS ORDRR_CELL_NO2 ,CASE WHEN LENGTH(C9.ORD_HTEL_TMP) >= 7 AND LENGTH(C9.ORD_HTEL_TMP) <=12 THEN ")
			 .append(" DECODE(LENGTH(C9.ORD_HTEL_TMP) ,12,SUBSTR(C9.ORD_HTEL_TMP,9,4) ,11,SUBSTR(C9.ORD_HTEL_TMP,8,4) ,10,SUBSTR(C9.ORD_HTEL_TMP,7,4) ")
			 .append(" ,9,SUBSTR(C9.ORD_HTEL_TMP,6,4) ,8,SUBSTR(C9.ORD_HTEL_TMP,5,4) ,7,SUBSTR(C9.ORD_HTEL_TMP,4,4) ,'0000') ELSE '0000' ")
			 .append(" END AS ORDRR_CELL_NO3 ,'' AS ORDRR_SAFE_NO1 ,'' AS ORDRR_SAFE_NO2 ,'' AS ORDRR_SAFE_NO3 ")
			 .append(" ,NVL(M1.ORD_ZIP_CD1 ,' ') || NVL(M1.ORD_ZIP_CD2 ,' ') AS ORDRR_ZIP_NO , REPLACE(NVL(M1.ORD_BASIC ,' '),CHR(39), '') AS ORDRR_ADDR ")
			 .append(" ,REPLACE(NVL(M1.ORD_BASIC ,' '),CHR(39),'') ||' '|| REPLACE(NVL(M1.ORD_DETAIL ,' '),CHR(39),'') AS ORDRR_DETAIL_ADDR ")
			 .append(" ,'' AS INVC_NO ,'' AS ORI_INVC_NO ,'' AS ORI_ORD_NO ,'' AS COLCT_EXPCT_YMD ,'' AS COLCT_EXPCT_HOUR ,'' AS SHIP_EXPCT_YMD ,'' AS SHIP_EXPCT_HOUR")
			 .append(" ,'01' AS PRT_ST ,'' AS ARTICLE_AMT ,NVL( REPLACE( REPLACE( REPLACE( REPLACE( REPLACE( REPLACE(M1.DELIVERY_MSG, CHR(13)||CHR(10) ,' '),'&NBSP;',' ') ")
			 .append(" ,'&AMP;','') ,'&LT;','') ,'&GT;','') ,'&QUOT;','') ,' ') AS REMARK_1 ,'' AS REMARK_2 ,'' AS REMARK_3 ,'' AS COD_YN ")
			 .append(" ,NVL(MAX(M2.ITEM_CD) ,' ') AS GDS_CD ,CASE WHEN COUNT(M2.ITEM_CD) = 1 THEN NVL(SUBSTRB(MAX(C4.ITEM_NM) ,1 ,40) ,' ') ELSE NVL(SUBSTRB( ")
			 .append(" (SELECT ITEM_NM FROM CMITEM WHERE BRAND_CD=M1.BRAND_CD AND ITEM_CD=( SELECT MAX(ITEM_CD) FROM LO010ND WHERE CENTER_CD=M1.CENTER_CD AND BRAND_CD=M1.BRAND_CD ")
			 .append(" AND ORDER_DATE=M1.ORDER_DATE AND ORDER_NO=M1.ORDER_NO ")
			 .append(" ) ) ||' 외 '|| CAST(COUNT(M2.ITEM_CD)-1 AS VARCHAR2(10)) ||'건' ,1 ,40) , ' ' ) END AS GDS_NM ")
			 .append("        ,1 AS GDS_QTY                            ")                    
			 .append("        ,'' AS UNIT_CD                           ")                     
			 .append("        ,'' AS UNIT_NM                          ")                      
			 .append("        ,'' AS GDS_AMT                         ")                       
			 .append("        ,'' AS ETC_1                         ")                        
			 .append("        ,'' AS ETC_2                        ")                         
			 .append("        ,'' AS ETC_3                       ")                          
			 .append("        ,'' AS ETC_4                      ")                           
			 .append("        ,'' AS ETC_5                     ")                            
			 .append("        ,'01' AS DLV_DV                  ")                             
			 .append("        ,'N' AS RCPT_ERR_YN               ")                              
			 .append("        ,'' AS RCPT_ERR_MSG              ")                              
			 .append("        ,'01' AS EAI_PRGS_ST             ")                               
			 .append("        ,'' AS EAI_ERR_MSG             ")                                
			 .append("        ,'ISEC' AS REG_EMP_ID        ")                                                                   
			 .append("        ,'ISEC' AS MODI_EMP_ID      ")                                    
			 .append("    FROM  LO010NM           M1 ")
			 .append("        JOIN    LO010ND     M2 ON M2.CENTER_CD  = M1.CENTER_CD ")
			 .append("                        AND M2.BRAND_CD   = M1.BRAND_CD ")
			 .append("                        AND M2.ORDER_DATE  = M1.ORDER_DATE ")
			 .append("                        AND M2.ORDER_NO   = M1.ORDER_NO ")
			 .append("        LEFT OUTER JOIN LO020NM   M3 ON M3.CENTER_CD  = M1.CENTER_CD ")
			 .append("                        AND M3.BRAND_CD   = M1.BRAND_CD ")
			 .append("                        AND M3.ORDER_DATE  = M1.ORDER_DATE ")
			 .append("                        AND M3.ORDER_NO   = M1.ORDER_NO  ")
			 .append("        JOIN    CMDELIVERY   C1 ON C1.BRAND_CD   = M1.BRAND_CD ")
			 .append("                        AND C1.DELIVERY_CD = M1.DELIVERY_CD ")
			 .append("        JOIN    CMDELIVERY   C2 ON C2.BRAND_CD   = M1.BRAND_CD ")
			 .append("                        AND C2.DELIVERY_CD = M1.REAL_DELIVERY_CD ")
			 .append("        JOIN    CMCODE     C3 ON C3.CODE_CD   = M1.INOUT_CD ")
			 .append("                        AND C3.CODE_GRP   = 'LDIV03'    ")
			 .append("                        AND C3.SUB_CD    IN ('E3')    ")
			 .append("        JOIN    CMITEM     C4 ON C4.BRAND_CD   = M2.BRAND_CD ")
			 .append("                        AND C4.ITEM_CD   = M2.ITEM_CD ")
			 .append("        JOIN    CMEXPRESSCONST C5 ON C5.BRAND_CD   = M1.BRAND_CD ")
			 .append("                        AND C5.CENTER_CD  = M1.CENTER_CD ")
			 .append("                        AND C5.EXPRESS_CD  = '09' ")
			 .append("        JOIN    CMBRAND     C6 ON C6.BRAND_CD   = M1.BRAND_CD ")
			 .append("        JOIN    CMCUST     C7 ON C7.CUST_CD   = C6.CUST_CD")
			 .append("        JOIN    CMCENTER    C8 ON C8.CENTER_CD  = M1.CENTER_CD ")
			 .append("        JOIN    CMDELIVERY   C10 ON C10.BRAND_CD  = M1.BRAND_CD ")
			 .append("                        AND C10.DELIVERY_CD = M1.FREE_VAL3                  ")
			 .append("        JOIN    ( ")
			 .append("                SELECT  ")
			 .append("                    A1.CENTER_CD ")
			 .append("                    ,A1.BRAND_CD ")
			 .append("                    ,A1.ORDER_DATE ")
			 .append("                    ,A1.ORDER_NO ")
			 .append("                    ,A2.LINE_NO ")
			 .append("                    ,TRANSLATE(NVL(A1.ACPER_TEL,'0'),'0123456789-)','0123456789') AS ACPER_TEL_TMP ")
			 .append("                    ,TRANSLATE(NVL(A1.ACPER_HP,'0'),'0123456789-)','0123456789') AS ACPER_HTEL_TMP ")
			 .append("                    ,TRANSLATE(NVL(A1.ORD_TEL,'0'),'0123456789-)','0123456789') AS ORD_TEL_TMP ")
			 .append("                    ,TRANSLATE(NVL(A1.ORD_HP,'0'),'0123456789-)','0123456789') AS ORD_HTEL_TMP ")
			 .append("                    ,CASE ")
			 .append("                      WHEN C3.FREE_VAL2 IS NULL THEN ")
			 .append("                        '0' ")
			 .append("                      ELSE ")
			 .append("                        TRANSLATE(C3.FREE_VAL2,'0123456789-)','0123456789') ")
			 .append("                     END  AS CENTER_TEL_TMP ")
			 .append("                FROM  LO010NM           A1 ")
			 .append("                    JOIN    LO010ND     A2 ON A2.CENTER_CD  = A1.CENTER_CD ")
			 .append("                                    AND A2.BRAND_CD   = A1.BRAND_CD ")
			 .append("                                    AND A2.ORDER_DATE  = A1.ORDER_DATE ")
			 .append("                                    AND A2.ORDER_NO   = A1.ORDER_NO ")
			 .append("                    LEFT OUTER JOIN LO020NM   A3 ON A3.CENTER_CD  = A1.CENTER_CD ")
			 .append("                                    AND A3.BRAND_CD   = A1.BRAND_CD ")
			 .append("                                    AND A3.ORDER_DATE  = A1.ORDER_DATE ")
			 .append("                                    AND A3.ORDER_NO   = A1.ORDER_NO ")
			 .append("                    JOIN    CMDELIVERY   C1 ON C1.BRAND_CD   = A1.BRAND_CD")
			 .append("                                    AND C1.DELIVERY_CD = A1.REAL_DELIVERY_CD")
			 .append("                    JOIN    CMEXPRESSCONST C2 ON C2.BRAND_CD   = A1.BRAND_CD")
			 .append("                                    AND C2.CENTER_CD  = A1.CENTER_CD ")
			 .append("                                    AND C2.EXPRESS_CD  = '09' ")
			 .append("                    JOIN    CMDELIVERY   C3 ON C3.BRAND_CD   = A1.BRAND_CD ")
			 .append("                                    AND C3.DELIVERY_CD = A1.FREE_VAL3 ")
			 .append("            JOIN    CMCENTER    C4 ON C4.CENTER_CD  = A1.CENTER_CD ")
			 .append(" WHERE  A1.CENTER_CD    LIKE '%' ")
			 .append("                AND   A1.BRAND_CD     LIKE '%' ")
			 .append("                AND   A1.ORDER_DATE    BETWEEN TO_DATE('20130325','YYYYMMDD') AND TRUNC(SYSDATE) ")
			 .append("                AND   A1.OUTBOUND_STATE  < '20' ")
			 .append("                AND   A3.OUTBOUND_STATE  IS NULL ")
			 .append("                AND   A1.PICKUP_YN        = 'Y' ")
			 .append("                AND   NOT EXISTS (SELECT CODE_CD FROM CMCODE WHERE CODE_GRP='LDIV25' AND CODE_CD=A1.FREE_VAL6) ")
			 .append("              ) C9  ON C9.CENTER_CD  = M1.CENTER_CD ")
			 .append("                  AND C9.BRAND_CD   = M1.BRAND_CD ")
			 .append("                  AND C9.ORDER_DATE  = M1.ORDER_DATE ")
			 .append("                  AND C9.ORDER_NO   = M1.ORDER_NO ")
			 .append("                  AND C9.LINE_NO   = M2.LINE_NO ")
			 .append("    WHERE  M1.CENTER_CD    LIKE '%' ")
			 .append("    AND   M1.BRAND_CD     LIKE '%' ")
			 .append("    AND   M1.ORDER_DATE    BETWEEN TO_DATE('20130325','YYYYMMDD') AND TRUNC(SYSDATE) ")
			 .append("    AND   M1.OUTBOUND_STATE  < '20' ")
			 .append("    AND   M1.FREE_VAL7    = '10'  ")
			 .append("    AND   M3.OUTBOUND_STATE  IS NULL")
			 .append("    AND   M1.PICKUP_YN = 'Y' ")
			 .append("    AND   NOT EXISTS (SELECT CODE_CD FROM CMCODE WHERE CODE_GRP='LDIV25' AND CODE_CD=M1.FREE_VAL6) ")
			 .append("    GROUP BY ")
			 .append("      M1.CENTER_CD")
			 .append("     ,M1.BRAND_CD")
			 .append("     ,M1.ORDER_DATE")
			 .append("     ,M1.ORDER_NO")
			 .append("     ,M1.INOUT_CD")
			 .append(" 	   ,M3.BOX_NO ")
			 .append("     ,M2.BRAND_DATE")
			 .append("     ,M2.BRAND_NO")
			 .append("     ,C5.CUSTOMER_CD")
			 .append("     ,C5.JOINT_CD")
			 .append("     ,M1.DELIVERY_MSG")
			 .append("     ,C8.CENTER_NM")
			 .append("     ,C8.TEL_NO")
			 .append("     ,C8.ZIP_CD1")
			 .append("     ,C8.ZIP_CD2")
			 .append("     ,C8.ADDR_BASIC")
			 .append("     ,C8.ADDR_DETAIL")
			 .append("     ,M1.ACPER_NM")
			 .append("     ,M1.ACPER_ZIP_CD1")
			 .append("     ,M1.ACPER_ZIP_CD2")
			 .append("     ,M1.ACPER_BASIC")
			 .append("     ,M1.ACPER_DETAIL")
			 .append("     ,M1.ORD_NM")
			 .append("     ,M1.ORD_ZIP_CD1")
			 .append("     ,M1.ORD_ZIP_CD2")
			 .append("     ,M1.ORD_BASIC")
			 .append("     ,M1.ORD_DETAIL")
			 .append("     ,C9.ACPER_TEL_TMP")
			 .append("     ,C9.ACPER_HTEL_TMP")
			 .append("     ,C9.ORD_TEL_TMP")
			 .append("     ,C9.ORD_HTEL_TMP ")
			 .append("     ,C9.CENTER_TEL_TMP ");
			
			pstmt = conn.prepareStatement(sb.toString());
			rs = pstmt.executeQuery();
			rsmd = rs.getMetaData();
			int colCnt = rsmd.getColumnCount();
			while (rs.next()) {
				String[] row = new String[colCnt];
				for(int i=1; i<=colCnt; i++) {
					row[i-1] = rs.getString(i);					
				}
				arrLrData.add(row);
			}
			
			if(arrLrData.size() > 0) {
				logger.info("selectLrData :: Success !");
			} else {
				logger.info("selectLrData :: No data !");
			}
			 
		} catch (Exception e) {
			try {
				conn.rollback();
			} catch (Exception e1) {
				loggerErr.error(e1.getMessage());
			}
			e.printStackTrace();
			logger.info(e.getMessage());
		}  finally {
			try {
				close();
			} catch (Exception e) {
				loggerErr.error(e.getMessage());
			}
		}
		
		logger.info("selectLrData :: End");
		return arrLrData;
	}
	
	public int insertLrData(Connection conn, ArrayList<String[]> arrLrData) {
		
		logger.info("insertLrData :: Start");
		StringBuilder sb= new StringBuilder();
		 int result = -1;
		 try {
			 sb.append(" insert into EDIEXPRESS_CJ_SEND values ( ?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?," +
				 		"?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?," +
				 		"?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?," +
				 		"?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?," +
				 		"?,?,?,?,SYSDATE,?,SYSDATE)" );
			 this.pstmt = conn.prepareStatement(sb.toString());
			 for(int i=0; i<arrLrData.size(); i++) {
				 for(int j=0; j<arrLrData.get(i).length; j++) {
					 pstmt.setString(j+1, arrLrData.get(i)[j]);
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
			 e.printStackTrace();
			 loggerErr.error(e.getMessage());
		 } finally {
			try {
				close();
			} catch (Exception e) {
				loggerErr.error(e.getMessage());
			}
		}
		 logger.info("insertLrData :: End");
		 return result;
	}
	
	public int insertRT(Connection conn) {
		logger.info("insertRT :: Start");
		int result = -1;
		StringBuilder sb = new StringBuilder();
		try {
			sb.append(" INSERT INTO ORDER_RETURN_DATA ")
			 .append(" SELECT M1.CENTER_CD ,M1.BRAND_CD ,M1.ORDER_DATE ,M1.ORDER_NO ,M1.INOUT_CD ,M2.BRAND_DATE ,M2.BRAND_NO, NULL, NULL, TRUNC(SYSDATE), SYSDATE ")                                     
			 .append(" FROM  LO010NM           M1 ")
			 .append("        JOIN    LO010ND     M2 ON M2.CENTER_CD  = M1.CENTER_CD ")
			 .append("                        AND M2.BRAND_CD   = M1.BRAND_CD ")
			 .append("                        AND M2.ORDER_DATE  = M1.ORDER_DATE ")
			 .append("                        AND M2.ORDER_NO   = M1.ORDER_NO ")
			 .append("        LEFT OUTER JOIN LO020NM   M3 ON M3.CENTER_CD  = M1.CENTER_CD ")
			 .append("                        AND M3.BRAND_CD   = M1.BRAND_CD ")
			 .append("                        AND M3.ORDER_DATE  = M1.ORDER_DATE ")
			 .append("                        AND M3.ORDER_NO   = M1.ORDER_NO  ")
			 .append("        JOIN    CMDELIVERY   C1 ON C1.BRAND_CD   = M1.BRAND_CD ")
			 .append("                        AND C1.DELIVERY_CD = M1.DELIVERY_CD ")
			 .append("        JOIN    CMDELIVERY   C2 ON C2.BRAND_CD   = M1.BRAND_CD ")
			 .append("                        AND C2.DELIVERY_CD = M1.REAL_DELIVERY_CD ")
			 .append("        JOIN    CMCODE     C3 ON C3.CODE_CD   = M1.INOUT_CD ")
			 .append("                        AND C3.CODE_GRP   = 'LDIV03'    ")
			 .append("                        AND C3.SUB_CD    IN ('E3')    ")
			 .append("        JOIN    CMITEM     C4 ON C4.BRAND_CD   = M2.BRAND_CD ")
			 .append("                        AND C4.ITEM_CD   = M2.ITEM_CD ")
			 .append("        JOIN    CMEXPRESSCONST C5 ON C5.BRAND_CD   = M1.BRAND_CD ")
			 .append("                        AND C5.CENTER_CD  = M1.CENTER_CD ")
			 .append("                        AND C5.EXPRESS_CD  = '09' ")
			 .append("        JOIN    CMBRAND     C6 ON C6.BRAND_CD   = M1.BRAND_CD ")
			 .append("        JOIN    CMCUST     C7 ON C7.CUST_CD   = C6.CUST_CD")
			 .append("        JOIN    CMCENTER    C8 ON C8.CENTER_CD  = M1.CENTER_CD ")
			 .append("        JOIN    CMDELIVERY   C10 ON C10.BRAND_CD  = M1.BRAND_CD ")
			 .append("                        AND C10.DELIVERY_CD = M1.FREE_VAL3                  ")
			 .append("        JOIN    ( ")
			 .append("                SELECT  ")
			 .append("                    A1.CENTER_CD ")
			 .append("                    ,A1.BRAND_CD ")
			 .append("                    ,A1.ORDER_DATE ")
			 .append("                    ,A1.ORDER_NO ")
			 .append("                    ,A2.LINE_NO ")
			 .append("                    ,TRANSLATE(NVL(A1.ACPER_TEL,'0'),'0123456789-)','0123456789') AS ACPER_TEL_TMP ")
			 .append("                    ,TRANSLATE(NVL(A1.ACPER_HP,'0'),'0123456789-)','0123456789') AS ACPER_HTEL_TMP ")
			 .append("                    ,TRANSLATE(NVL(A1.ORD_TEL,'0'),'0123456789-)','0123456789') AS ORD_TEL_TMP ")
			 .append("                    ,TRANSLATE(NVL(A1.ORD_HP,'0'),'0123456789-)','0123456789') AS ORD_HTEL_TMP ")
			 .append("                    ,CASE ")
			 .append("                      WHEN C3.FREE_VAL2 IS NULL THEN ")
			 .append("                        '0' ")
			 .append("                      ELSE ")
			 .append("                        TRANSLATE(C3.FREE_VAL2,'0123456789-)','0123456789') ")
			 .append("                     END  AS CENTER_TEL_TMP ")
			 .append("                FROM  LO010NM           A1 ")
			 .append("                    JOIN    LO010ND     A2 ON A2.CENTER_CD  = A1.CENTER_CD ")
			 .append("                                    AND A2.BRAND_CD   = A1.BRAND_CD ")
			 .append("                                    AND A2.ORDER_DATE  = A1.ORDER_DATE ")
			 .append("                                    AND A2.ORDER_NO   = A1.ORDER_NO ")
			 .append("                    LEFT OUTER JOIN LO020NM   A3 ON A3.CENTER_CD  = A1.CENTER_CD ")
			 .append("                                    AND A3.BRAND_CD   = A1.BRAND_CD ")
			 .append("                                    AND A3.ORDER_DATE  = A1.ORDER_DATE ")
			 .append("                                    AND A3.ORDER_NO   = A1.ORDER_NO ")
			 .append("                    JOIN    CMDELIVERY   C1 ON C1.BRAND_CD   = A1.BRAND_CD")
			 .append("                                    AND C1.DELIVERY_CD = A1.REAL_DELIVERY_CD")
			 .append("                    JOIN    CMEXPRESSCONST C2 ON C2.BRAND_CD   = A1.BRAND_CD")
			 .append("                                    AND C2.CENTER_CD  = A1.CENTER_CD ")
			 .append("                                    AND C2.EXPRESS_CD  = '09' ")
			 .append("                    JOIN    CMDELIVERY   C3 ON C3.BRAND_CD   = A1.BRAND_CD ")
			 .append("                                    AND C3.DELIVERY_CD = A1.FREE_VAL3 ")
			 .append("            JOIN    CMCENTER    C4 ON C4.CENTER_CD  = A1.CENTER_CD ")
			 .append(" WHERE  A1.CENTER_CD    LIKE '%' ")
			 .append("                AND   A1.BRAND_CD     LIKE '%' ")
			 .append("                AND   A1.ORDER_DATE    BETWEEN TO_DATE('20130325','YYYYMMDD') AND TRUNC(SYSDATE) ")
			 .append("                AND   A1.OUTBOUND_STATE  < '20' ")
			 .append("                AND   A3.OUTBOUND_STATE  IS NULL ")
			 .append("                AND   A1.PICKUP_YN        = 'Y' ")
			 .append("                AND   NOT EXISTS (SELECT CODE_CD FROM CMCODE WHERE CODE_GRP='LDIV25' AND CODE_CD=A1.FREE_VAL6) ")
			 .append("              ) C9  ON C9.CENTER_CD  = M1.CENTER_CD ")
			 .append("                  AND C9.BRAND_CD   = M1.BRAND_CD ")
			 .append("                  AND C9.ORDER_DATE  = M1.ORDER_DATE ")
			 .append("                  AND C9.ORDER_NO   = M1.ORDER_NO ")
			 .append("                  AND C9.LINE_NO   = M2.LINE_NO ")
			 .append("    WHERE  M1.CENTER_CD    LIKE '%' ")
			 .append("    AND   M1.BRAND_CD     LIKE '%' ")
			 .append("    AND   M1.ORDER_DATE    BETWEEN TO_DATE('20130325','YYYYMMDD') AND TRUNC(SYSDATE) ")
			 .append("    AND   M1.OUTBOUND_STATE  < '20' ")
			 .append("    AND   M1.FREE_VAL7    = '10'  ")
			 .append("    AND   M3.OUTBOUND_STATE  IS NULL")
			 .append("    AND   M1.PICKUP_YN        = 'Y' ")
			 .append("    AND   NOT EXISTS (SELECT CODE_CD FROM CMCODE WHERE CODE_GRP='LDIV25' AND CODE_CD=M1.FREE_VAL6) ")
			 .append("    GROUP BY ")
			 .append("      M1.CENTER_CD")
			 .append("      ,M1.BRAND_CD")
			 .append("      ,M1.ORDER_DATE")
			 .append("      ,M1.ORDER_NO")
			 .append("      ,M1.INOUT_CD")
			 .append("      ,M2.BRAND_DATE")
			 .append("      ,M2.BRAND_NO")
			 .append("      ,C5.CUSTOMER_CD")
			 .append("      ,C5.JOINT_CD")
			 .append("      ,M1.DELIVERY_MSG")
			 .append("      ,C8.CENTER_NM")
			 .append("      ,C8.TEL_NO")
			 .append("      ,C8.ZIP_CD1")
			 .append("      ,C8.ZIP_CD2")
			 .append("      ,C8.ADDR_BASIC")
			 .append("      ,C8.ADDR_DETAIL")
			 .append("      ,M1.ACPER_NM")
			 .append("      ,M1.ACPER_ZIP_CD1")
			 .append("      ,M1.ACPER_ZIP_CD2")
			 .append("      ,M1.ACPER_BASIC")
			 .append("      ,M1.ACPER_DETAIL")
			 .append("      ,M1.ORD_NM")
			 .append("      ,M1.ORD_ZIP_CD1")
			 .append("      ,M1.ORD_ZIP_CD2")
			 .append("      ,M1.ORD_BASIC")
			 .append("      ,M1.ORD_DETAIL")
			 .append("      ,C9.ACPER_TEL_TMP")
			 .append("      ,C9.ACPER_HTEL_TMP")
			 .append("      ,C9.ORD_TEL_TMP")
			 .append("      ,C9.ORD_HTEL_TMP ")
			 .append("      ,C9.CENTER_TEL_TMP ");
			
			 pstmt = conn.prepareStatement(sb.toString());
			 result = pstmt.executeUpdate();
			 sb.delete(0,sb.length());
			 
		} catch (Exception e) {
			try {
				conn.rollback();
			} catch (Exception e1) {
				loggerErr.error(e1.getMessage());
			}
			e.printStackTrace();
			logger.info(e.getMessage());
		}  finally {
			try {
				close();
			} catch (Exception e) {
				loggerErr.error(e.getMessage());
			}
		}
		logger.info("insertRT :: End");
		return result;
	}
	
	
	public int runCJ(Connection connCJ, ArrayList<String[]> arrLrData){
		logger.info("runCJ :: Start");
		StringBuilder sb = new StringBuilder();
		int result = -1;
		try {
			logger.info("run_CJ :: Start !");
			String sendrDetailAddr = "";
			String rcvrDetailAddr = "";
			String ordrrDetailAddr = "";
			pstmt = connCJ.prepareStatement(sb.toString());
			for(int i=0; i<arrLrData.size(); i++) {
				sendrDetailAddr = arrLrData.get(i)[29];
				rcvrDetailAddr  = arrLrData.get(i)[42];			
				ordrrDetailAddr = arrLrData.get(i)[55];
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
				rs = pstmt.executeQuery(sb.toString());
				while(rs.next()) {
					arrLrData.get(i)[28] = rs.getString("SENDR_ADDR");
					arrLrData.get(i)[29] = rs.getString("SENDR_DETAIL_ADDR");
					arrLrData.get(i)[41] = rs.getString("RCVR_ADDR");
					arrLrData.get(i)[42] = rs.getString("RCVR_DETAIL_ADDR");
					arrLrData.get(i)[54] = rs.getString("ORDRR_ADDR");
					arrLrData.get(i)[55] = rs.getString("ORDRR_DETAIL_ADDR");
					break;	
				}
				sb.delete(0, sb.length());
			}
			
			result = insertCJ(connCJ, arrLrData);
		} catch (Exception e) {
			loggerErr.error(e.getMessage());
			try {
				connCJ.rollback();
			} catch (Exception e1) {
				loggerErr.error(e.getMessage());
			}
			e.printStackTrace();
		} finally {
			try {
				close();
			} catch (Exception e) {
				loggerErr.error(e.getMessage());
			}
		}
		logger.info("runCJ :: End");
		return result;
	}
	
	private int insertCJ(Connection connCJ, ArrayList<String[]> arrLrData) throws Exception {
		 logger.info("insertCJ :: Start");
		 StringBuilder sb= new StringBuilder();
		 int result = -1;
		 try {
			 sb.append(" insert into V_RCPT_ISEC010 values ( ?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?," +
				 		"?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?," +
				 		"?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?," +
				 		"?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?," +
				 		"?,?,?,?, SYSDATE,?, SYSDATE)" );
			 this.pstmt = connCJ.prepareStatement(sb.toString());
			 for(int i=0; i<arrLrData.size(); i++) {

				 System.out.println("############################S");
				 for(int j=2; j<arrLrData.get(i).length; j++) {
					 pstmt.setString((j-1), arrLrData.get(i)[j]);
					 System.out.println(arrLrData.get(i)[j]);
				 }
				 System.out.println("############################E");
				 result = pstmt.executeUpdate();
				 if(result < 1) {
					 break;
				 }
			 }
		 } catch (Exception e) {
			 try {
				 result = -1;
				 connCJ.rollback();
			 } catch (Exception e1) {
				 result = -1;
				 loggerErr.error(e1.getMessage());
			 } 
			 loggerErr.error(e.getMessage());
			 e.printStackTrace();
		 }
		 logger.info("insertCJ :: End");		 
		 return result;
	 }
	
	public int updateTargetDone(Connection conn) {
		logger.info("updateTargetDone :: Start");
		int result = -1;
		StringBuilder sb = new StringBuilder();
		try {
			sb.append(" UPDATE LO010NM SET FREE_VAL7 = '60' WHERE FREE_VAL7 = '10' ");
			pstmt = conn.prepareStatement(sb.toString());
			
			result = pstmt.executeUpdate();
		} catch (Exception e) {
			loggerErr.error(e.getMessage());
			e.printStackTrace();
		} finally {
			try {
				close();
			} catch (Exception e) {
				loggerErr.error(e.getMessage());
			}
		}
		logger.info("updateTargetDone :: End");
		return result;
	}
	
	public int updateTargetCancel(Connection conn) {
		logger.info("updateTargetCancel :: Start");
		StringBuilder sb = new StringBuilder();
		int result = -1;
		try {
			sb.append(" UPDATE LO010NM SET FREE_VAL7 = '00' WHERE FREE_VAL7 = '10' ");
			pstmt = conn.prepareStatement(sb.toString());
			result = pstmt.executeUpdate();
		} catch (Exception e) {
			try {
				conn.rollback();
			} catch (Exception e1) {
				loggerErr.error(e.getMessage());
			}
			e.printStackTrace();
		} finally {
			try {
				close();
			} catch (Exception e) {
				loggerErr.error(e.getMessage());
			}
		}
		logger.info("updateTargetCancel :: End");
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
 			e.printStackTrace();
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
