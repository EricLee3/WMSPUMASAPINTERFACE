package com.gics.edi.linker;

import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Properties;

import oracle.jdbc.driver.OracleTypes;
import oracle.jdbc.internal.OracleCallableStatement;

import org.apache.log4j.Logger;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.NodeList;

import com.gics.edi.common.PropManager;
import com.gics.edi.common.XMLManager;
import com.gics.edi.conn.ConnectionManager;
import com.gics.edi.customize.puma.PumaLinker;

public class Linker {
	private Connection conn = null;
	private CallableStatement cs = null;
	private OracleCallableStatement ocstmt = null;
	private static final String _VALUE_TEXT = "$value$";
	private ArrayList<String> keyList = null;
	private boolean isOtherProcess = false;
	
	private static Logger loggerErr = Logger.getLogger("process.puma.err");
	private static Logger logger = Logger.getLogger("process.puma");
	
	public String callWMSProc(String propName, String path) {
		ConnectionManager connMgr = ConnectionManager.getInstances();
		
		StringBuilder sb = new StringBuilder();
		PropManager propMgr = PropManager.getInstances();
		Properties prop = propMgr.getProp(propName, "puma");
		String brandCd = prop.getProperty(propName+".brandCd");
		String procNm = getProcName(propName, path);
		String type = prop.getProperty(propName+".type");
		
		logger.info("procNm: "+procNm);
		String errCd = "";
		String errCts = "";
		
		sb.append(" call ")
		  .append(procNm)
		  .append("(?,?,?,?");
		int procCnt = Integer.parseInt(prop.getProperty(propName+".procParamCnt"));
		for(int i=0; i<procCnt; i++) {
			sb.append(",?");
		}
		sb.append(")");
		logger.info("query: "+sb.toString());
		
		try {
			conn = connMgr.connect("wmsdb");
			cs = conn.prepareCall(sb.toString());
			
			String[][] recvData = null;
			if("KEY".equals(type)) {
				recvData = createKeyTypeData(propName, path);
			} else {
				recvData = createNonKeyTypeData(propName, path);
			}
			if(isOtherProcess()) {
				setOtherProcess(false);
				return "F";
			}
			for(int i=0; i<recvData.length; i++) {
				cs.registerOutParameter(1, OracleTypes.VARCHAR);
				cs.registerOutParameter(2, OracleTypes.VARCHAR);
				if(i>=(recvData.length-1)) {
					cs.setString(3, "Y"); // LAST_YN
				} else {
					cs.setString(3, "N"); // LAST_YN
				}
				
				cs.setString(4, brandCd); // BRAND_CD
				for(int j=0; j<procCnt; j++) {
					if(j<recvData[i].length) {
						cs.setString((j+1)+4, recvData[i][j]);
					} else {
						cs.setString((j+1)+4, null);
					}
				}
				cs.execute();
				
				
				ocstmt = (OracleCallableStatement)cs;
	            errCd = (String)ocstmt.getString(1);
	            errCts = (String)ocstmt.getString(2);
	            
	            logger.info("errCd: "+errCd);
	            logger.info("errCts: "+errCts);
			}
			
			if("recvItem".equals(propName)) {
				for(int i=0; i<recvData.length; i++) {
					PumaLinker pumaLinker = new PumaLinker();
					pumaLinker.updateItemStyle(recvData[i][0], recvData[i][4]);
					pumaLinker.updateKRSize(path);
				}
			}
		} catch (Exception e) {
			e.printStackTrace();
			loggerErr.error(e.getMessage());
		} finally {
			try {
				if(cs!=null) {
					cs.close();
				}
				if(conn!=null) {
					conn.close();
				}
			} catch (Exception e) {
				loggerErr.error(e.getMessage());
			}
		}
		return errCd;
	}	
	
	private String getProcName(String propName, String path) {
		XMLManager xmlMgr = new XMLManager();
		Document doc = xmlMgr.read(path);
		String result = "";
		PropManager propMgr = PropManager.getInstances();
		Properties prop = propMgr.getProp(propName, "puma");
		keyList = getKeyList(doc, propName);
		try {
			String tableNm = "";
			String columnNm = "";
			String condition = "";
			if(prop.getProperty(propName+".procNm").split("%").length>1) {
				tableNm = prop.getProperty(propName+".procNm").split("%")[0];
				columnNm = prop.getProperty(propName+".procNm").split("%")[1];
			}
			NodeList nodeList = doc.getElementsByTagName(tableNm);
			if(tableNm.contains(_VALUE_TEXT)) {
				result = columnNm.replaceAll(_VALUE_TEXT, "");
			} else {
				if(prop.getProperty(propName+".proc.condition")!=null) {
					condition = prop.getProperty(propName+".proc.condition");
				}
				for(int i=0; i<nodeList.getLength(); i++) {
					Element element = (Element) nodeList.item(i);
					if(element != null) {
						String value = getChildren(element, columnNm, condition, null, i, nodeList.getLength())[1];
						result = value;
					}
				}
			}
		} catch (Exception e) {
			loggerErr.error(e.getMessage());
		}
		return result;
	}
	public String[][] createNonKeyTypeData(String propName, String path) {
		XMLManager xmlMgr = new XMLManager();
		Document doc = xmlMgr.read(path);
		PropManager propMgr = PropManager.getInstances();
		Properties prop = propMgr.getProp(propName, "puma");
		keyList = getKeyList(doc, propName);
		String[][] result = new String[keyList.size()][Integer.parseInt(prop.getProperty(propName+".totalCnt"))];
		try {
			int colCnt=1;
			while(prop.getProperty(propName+".col"+colCnt)!=null) {
				String tableNm = "";
				String columnNm = "";
				String condition = "";
				String mValue = "";
				if(prop.getProperty(propName+".col"+colCnt).split("%").length>1) {
					if(prop.getProperty(propName+".col"+colCnt).indexOf("<")>0) {
						tableNm = prop.getProperty(propName+".col"+colCnt).substring(0,
								prop.getProperty(propName+".col"+colCnt).indexOf("<")-1).split("%")[0];
						columnNm = prop.getProperty(propName+".col"+colCnt).substring(0,
								prop.getProperty(propName+".col"+colCnt).indexOf("<")-1).split("%")[1];
					} else {
						tableNm = prop.getProperty(propName+".col"+colCnt).split("%")[0];
						columnNm = prop.getProperty(propName+".col"+colCnt).split("%")[1];
					}
				}
				
				NodeList nodeList = doc.getElementsByTagName(tableNm);
				if(tableNm.contains(_VALUE_TEXT)) {
					for(int i=0; i<keyList.size(); i++) {
						result[i][colCnt-1] = columnNm.replaceAll(_VALUE_TEXT, "");
					}
				} else {
					if(prop.getProperty(propName+".condition.col"+colCnt)!=null) {
						condition = prop.getProperty(propName+".condition.col"+colCnt);
					}
					for(int i=0; i<nodeList.getLength(); i++) {
//					for(int i=0; i<2; i++) {
						Element element = (Element) nodeList.item(i);
						if(element != null) {
							String value = getChildren(element, columnNm, condition, null, i, nodeList.getLength())[1];
//							System.out.println("value: "+value);
							if(prop.getProperty(propName+".col"+colCnt).contains("$master")) {
								if(value!=null&&!"null".equals(value)) {
									result[0][colCnt-1] = value;
								}
							} else {
								result[i][colCnt-1] = value;
							}
							if(value!=null && value.length() > 0) {
								if(value.contains("$otherProcess")) {
//									System.out.println("otherProcess");
									isOtherProcess = true;
									break;
								}
								mValue = value;
							}
						}
					}
					
					if(isOtherProcess) {
						break;
					}
				}
				
				
				if(prop.getProperty(propName+".col"+colCnt).contains("$master")) {
					for(int i=0; i<result.length; i++) {
						if(result[i][colCnt-1]==null) {
							result[i][colCnt-1]=mValue;
						}
					}
				}					
				colCnt++;
			}
			
//			System.out.println("isOtherProcess: "+ isOtherProcess);
			for(int i=0; i<result.length; i++) {
				for(int j=0; j<result[i].length; j++){
					System.out.print(result[i][j]+" ");
				}
				System.out.print("\n");
			}
			
			setOtherProcess(isOtherProcess);
		} catch (Exception e) {
			loggerErr.error(e.getMessage());
		}
		return result;
	}
	public String[][] createKeyTypeData(String propName, String path) {
		XMLManager xmlMgr = new XMLManager();
		Document doc = xmlMgr.read(path);
		PropManager propMgr = PropManager.getInstances();
		Properties prop = propMgr.getProp(propName, "puma");
		keyList = getKeyList(doc, propName);
		String[][] result = new String[keyList.size()][Integer.parseInt(prop.getProperty(propName+".totalCnt"))];
		try {
			int colCnt=1;
			while(prop.getProperty(propName+".col"+colCnt)!=null) {
				String tableNm = "";
				String columnNm = "";
				String condition = "";
				if(prop.getProperty(propName+".col"+colCnt).split("%").length>1) {
					
					tableNm = prop.getProperty(propName+".col"+colCnt).split("%")[0];
					columnNm = prop.getProperty(propName+".col"+colCnt).split("%")[1];
				}
				NodeList nodeList = doc.getElementsByTagName(tableNm);
				if(tableNm.contains(_VALUE_TEXT)) {
					for(int i=0; i<keyList.size(); i++) {
						result[i][colCnt-1] = columnNm.replaceAll(_VALUE_TEXT, "");
					}
				} else {
					if(prop.getProperty(propName+".condition.col"+colCnt)!=null) {
						condition = prop.getProperty(propName+".condition.col"+colCnt);
					}

					for(int i=0; i<keyList.size(); i++) {
						for(int j=0; j<nodeList.getLength(); j++) {
							if(result[i][colCnt-1]!=null) {
								break;
							}
							Element element = (Element) nodeList.item(j);
							if(element != null) {
								String value[] = getChildren(element, columnNm, condition, keyList.get(i), j, nodeList.getLength());
								result[i][colCnt-1] = value[1];
							}
						}
					}
				}
				colCnt++;
			}
//			for(int i=0; i<result.length; i++) {
//				for(int j=0; j<result[i].length; j++){
//					System.out.print(result[i][j]+" ");
//				}
//				System.out.print("\n");
//			}
		} catch (Exception e) {
			loggerErr.error(e.getMessage());
		}
		return result;
	}
	public int getKeyIndex(String key) {
		int rtnValue = -1;
		for(int i=0; i<keyList.size(); i++) {
			if(key.equals(keyList.get(i))) {
				rtnValue = i;
				break;
			}
		}
		return rtnValue;
	}
	public void createSendData() {
	}
	public ArrayList<String> getKeyList(Document doc, String propName) {
		ArrayList<String> list = new ArrayList<String>();
		PropManager propMgr = PropManager.getInstances();
		Properties prop = propMgr.getProp(propName, "puma");
		int i=1;
		while(prop.getProperty(propName+".keyCol"+i) != null) {
			String tableNm="";
			String columnNm="";
			if(prop.getProperty(propName+".keyCol"+i).split("%").length>1) {
				tableNm = prop.getProperty(propName+".keyCol"+i).split("%")[0];
				columnNm = prop.getProperty(propName+".keyCol"+i).split("%")[1];
			}
			NodeList nodeList = doc.getElementsByTagName(tableNm);
			for(int n=0; n<nodeList.getLength(); n++) {
				Element element = (Element) nodeList.item(n);
				list.add(getChildren(element, columnNm));
			}
			i++;
		}
		return list;
	}
	public String getChildren(Element element, String tagName) {
		NodeList list = element.getElementsByTagName(tagName);
		Element cElement = (Element) list.item(0);
		if(cElement == null) {
			return "";
		}
		if(cElement.getFirstChild()!=null) {
			return cElement.getFirstChild().getNodeValue().replaceAll("\\^", " ");				
		} else {
			return "";
		}
	}
	public String[] getChildren(Element element, String tagName, String conditions, String key, int currIdx, int nodeSize) {
		
	
		String[] rtnValue = new String[2]; // 0: key , 1: value
		ArrayList<String> condition = new ArrayList<String>();
		ArrayList<String> value = new ArrayList<String>();
		ArrayList<String> cndValue = new ArrayList<String>();
		NodeList cndNodeList = null; 
		Element cndElement = null;
		
		String[] cValues = new String[2];
		NodeList list = null;
		Element cElement = null;
		if(!tagName.contains("$cValue")) {
			list = element.getElementsByTagName(tagName);	
			cElement = (Element) list.item(0);
			if(cElement == null) {
				return rtnValue;
			}
		}
		
		if(conditions.length()>0) {
			if(conditions.contains("&")) {
				for(int i=0; i<conditions.split("&").length; i++) {
					condition.add(conditions.split("&")[i]);
				}
			} else if(conditions.contains("|")) {
				for(int i=0; i<conditions.split("[|]").length; i++) {
					condition.add(conditions.split("[|]")[i]);
				}
			} else {
				condition.add(conditions);
			}
			
			if(tagName.contains("$cValue")) {
				if(condition.size() > 0) {
					if(condition.get(condition.size()-1).split("=")[1].split("<>")[0].contains("$null")) {
						cValues[0] = null;
					} else {
						cValues[0] = condition.get(condition.size()-1).split("=")[1].split("<>")[0];
					}
					
					if(condition.get(condition.size()-1).split("=")[1].split("<>")[1].contains("$null")) {
						cValues[1] = null;
					} else {
						cValues[1] = condition.get(condition.size()-1).split("=")[1].split("<>")[1];
					}
					condition.set(condition.size()-1, condition.get(condition.size()-1).substring(0, condition.get(condition.size()-1).indexOf("=")));
				} 
			}
			
			for(int i=0; i<condition.size(); i++) {
				String[] tmp = condition.get(i).split("%");
				if(tmp[1].contains("$key")) {
					tmp[1] = tmp[1].replaceAll("\\{","").replaceAll("\\}","").replaceAll("\\$","");
					cndValue.add(key);
				} else {
					cndValue.add(tmp[1]);
				}
				
				if(cndValue.get(cndValue.size()-1).contains("subStr")
					|| cndValue.get(cndValue.size()-1).contains("split")	
				) {
					cndValue.remove(cndValue.size()-1);
				}
				
				cndNodeList = element.getElementsByTagName(tmp[0]);
				cndElement = (Element) cndNodeList.item(0);
				if(cndElement==null) {
					return rtnValue;
				}
				
				if(cndElement.getFirstChild()!=null) {
					value.add(cndElement.getFirstChild().getNodeValue());
				}
			}
			
			if(tagName.contains("$cValue")) {
//				System.out.println("$cValue");
				if(conditions.contains("|")) {
					boolean isExist = false;
					for(int i=0; i<cndValue.size(); i++) {
						if(cndValue.get(i).equals(value.get(i))) {
							isExist = true;
							break;
						}
					}
					
					if(isExist) {
//						System.out.println("or if--");
						rtnValue[0] = key;
						rtnValue[1] = cValues[0];
					} else {
//						System.out.println("or else--");
						//if((currIdx+1)>=nodeSize-1){
//							System.out.println("or else1--");
							rtnValue[0] = key;
							rtnValue[1] = cValues[1];
						//}
					}
				} else {
					boolean isExist = true;
					for(int i=0; i<cndValue.size(); i++) {
//						System.out.println("cndValue.get(i): "+ cndValue.get(i));
//						System.out.println("value.get(i): "+ value.get(i));
						if(!cndValue.get(i).equals(value.get(i))) {
							isExist = false;
							break;
						}
					}
					if(isExist) {
//						System.out.println("and if--");
						rtnValue[0] = key;
						rtnValue[1] = cValues[0];
					} else {
//						System.out.println("and else--");
						//if((currIdx+1)>=nodeSize-1){
//							System.out.println("and else1--");
							rtnValue[0] = key;
							rtnValue[1] = cValues[1];
						//}
					}
				}
			} else {
				if(cElement.getFirstChild()!=null) {
					if(conditions.contains("|")) {
						boolean isExist = false;
						for(int i=0; i<cndValue.size(); i++) {
							if(!cndValue.get(i).equals(value.get(i))) {
								isExist = true;
								break;
							}
						}
						
						if(isExist) {
							rtnValue[0] = key;
							rtnValue[1] = cElement.getFirstChild().getNodeValue();
						}
					} else {
						boolean isExist = true;
						for(int i=0; i<cndValue.size(); i++) {
							if(!cndValue.get(i).equals(value.get(i))) {
								isExist = false;
								break;
							}
						}
						
						if(isExist) {
							rtnValue[0] = key;
							rtnValue[1] = cElement.getFirstChild().getNodeValue();
						}
					}
				}
			}
		} else {
			if(cElement.getFirstChild()!=null) {
				rtnValue[0] = key;
				rtnValue[1] = cElement.getFirstChild().getNodeValue();				
			}
		}
		if(rtnValue[1] != null) {
			if(conditions.length() > 0) {
				if(conditions.contains("$subStr")) {
					int stIdx = Integer.parseInt(conditions.trim().substring(conditions.trim().indexOf("$subStr", 1), conditions.trim().length()).replaceAll("\\$", "")
							  								.replaceAll("subStr", "").replaceAll("\\(", "").replaceAll("\\)", "").replaceAll("}", "").split(",")[0]);
					int endIdx = Integer.parseInt(conditions.trim().substring(conditions.trim().indexOf("$subStr", 1), conditions.trim().length()).replaceAll("\\$", "")
															.replaceAll("subStr", "").replaceAll("\\(", "").replaceAll("\\)", "").replaceAll("}", "").split(",")[1]);
					rtnValue[1] = rtnValue[1].substring(stIdx, endIdx);
				}
				
				if(conditions.contains("$split")) {
					String regEx = conditions.trim().substring(conditions.trim().indexOf("$split", 1), conditions.trim().length()).replaceAll("\\$", "")
									.replaceAll("split", "").replaceAll("\\(", "")
									.replaceAll("\\)", "").replaceAll("}", "").replaceAll("delSymbols", "").split(",")[0];
					int inx = Integer.parseInt(conditions.trim().substring(conditions.trim().indexOf("$split", 1), conditions.trim().length())
												.replaceAll("\\$", "").replaceAll("split", "").replaceAll("\\(", "").replaceAll("\\{", "")
												.replaceAll("\\)", "").replaceAll("}", "").replaceAll("delSymbols", "").replaceAll("%", "")
												.split(",")[1]);
					rtnValue[1] = rtnValue[1].split("\\"+regEx)[inx];
				}
				
				if(conditions.contains("$delSymbols")) {
					rtnValue[1] = rtnValue[1].substring(0, rtnValue[1].indexOf(","));
				}
			}
		}
	
		return rtnValue;
	}
	
	public ArrayList<String[]> createSendData(String propName, String div) {
		
		PropManager propMgr = PropManager.getInstances();
		Properties prop = propMgr.getProp(propName, div);
		Connection conn = null;
		PreparedStatement pstmt = null;
		ResultSet rs = null;
		ConnectionManager connMgr = ConnectionManager.getInstances();
		StringBuilder sb = new StringBuilder();
		ArrayList<String[]> sendData = new ArrayList<String[]>();
		try {
			conn = connMgr.connect("wmsdb");
			sb.append(prop.getProperty(propName+".query"));
			
			pstmt = conn.prepareStatement(sb.toString());
			logger.info(sb.toString());
			
			rs = pstmt.executeQuery();
			int rowLenght = rs.getMetaData().getColumnCount();
			while(rs.next()) {
				String[] row = new String[rowLenght];
				for(int i=1; i<=rowLenght; i++) {
					row[i-1] = rs.getString(i);
				}
				sendData.add(row);
			}
		} catch (SQLException e) {
			loggerErr.error(e.getMessage());
		} catch (Exception e) {
			loggerErr.error(e.getMessage());
		} finally {
			try {
				if(rs!=null) {
					rs.close();
				}
				if(pstmt!=null) {
					pstmt.close();
				}
				if(conn!=null) {
					conn.close();
				}
			} catch (Exception e) {
				loggerErr.error(e.getMessage());
			}
		}
		return sendData;
	}
	
	public ArrayList<String[]> createSendData(String propName, String div, String outboundNo) {
		Connection conn = null;
		PreparedStatement pstmt = null;
		ResultSet rs = null;
		ConnectionManager connMgr = ConnectionManager.getInstances();
		StringBuilder sb = new StringBuilder();
		ArrayList<String[]> sendData = new ArrayList<String[]>();
		try {
			conn = connMgr.connect("wmsdb");
			if("sendInbound".equals(propName)) {
								
				sb.append(" SELECT 'EDI_DC40','100','700','2','MBGMCR02','MBGMCR'")
				.append(" ,'MBGMCR','ISE-TRN','LS','KRISE','SAPT05','LS','T05CLNT100' ")
				.append(" ,M1.CENTER_CD||M1.BRAND_CD||TO_CHAR(M1.INBOUND_DATE,'YYYYMMDD')||M1.INBOUND_NO||M2.LINE_NO ORDER_SEQ")
				.append(" ,M1.FREE_VAL3 BRAND_DATE ")
				.append(" ,TO_CHAR(SYSDATE, 'YYYYMMDD') INBOUND_DATE, M1.BRAND_NO ")
				.append(" ,M1.CENTER_CD||M1.BRAND_CD||TO_CHAR(M1.INBOUND_DATE,'YYYYMMDD')||M1.INBOUND_NO||M2.LINE_NO ORDER_SEQ2")
				.append(" ,'01','101'")
				.append(" ,M2.ENTRY_QTY,'B',M1.BRAND_NO,M2.BRAND_LINE_NO ") 
				.append(" FROM LI020NM M1, LI020ND M2 ")
				.append(" WHERE  M1.CENTER_CD = M2.CENTER_CD AND M1.BRAND_CD = M2.BRAND_CD ") 
				.append(" AND M1.INBOUND_DATE = M2.INBOUND_DATE ") 
				.append(" AND M1.INBOUND_NO = M2.INBOUND_NO ") 
				.append(" AND M1.CENTER_CD = 'A1' ") 
				.append(" AND M1.BRAND_CD = '6101' ") 
				.append(" AND M1.INBOUND_DATE <= TRUNC(SYSDATE) ")
				.append(" AND M1.INBOUND_NO = '").append(outboundNo).append("' ")
				.append(" AND M1.INBOUND_STATE='60' ") 
				.append(" AND ( M1.SEND_STATE = '00' OR M1.SEND_STATE IS NULL ) ");
				
			}  else if("sendInbound1".equals(propName)) {
				
				/**
				sb.append(" SELECT 'EDI_DC40','100','700','2','MBGMCR02','MBGMCR'")
				.append(" ,'MBGMCR','ISE-TRN','LS','KRISE','SAPT05','LS','T05CLNT100' ")
				.append(" ,M1.CENTER_CD||M1.BRAND_CD||TO_CHAR(M1.INBOUND_DATE,'YYYYMMDD')||M1.INBOUND_NO||M2.LINE_NO ORDER_SEQ")
				.append(" ,M1.FREE_VAL3 BRAND_DATE ")
				.append(" ,TO_CHAR(SYSDATE, 'YYYYMMDD') INBOUND_DATE, M1.BRAND_NO ")
				.append(" ,M1.CENTER_CD||M1.BRAND_CD||TO_CHAR(M1.INBOUND_DATE,'YYYYMMDD')||M1.INBOUND_NO||M2.LINE_NO ORDER_SEQ2")
				.append(" ,'01','101'")
				.append(" ,M2.ENTRY_QTY,'B',M1.BRAND_NO,M2.BRAND_LINE_NO ") 
				.append(" FROM LI020NM M1, LI020ND M2 ")
				.append(" WHERE  M1.CENTER_CD = M2.CENTER_CD AND M1.BRAND_CD = M2.BRAND_CD ") 
				.append(" AND M1.INBOUND_DATE = M2.INBOUND_DATE ") 
				.append(" AND M1.INBOUND_NO = M2.INBOUND_NO ") 
				.append(" AND M1.CENTER_CD = 'A1' ") 
				.append(" AND M1.BRAND_CD = '6101' ") 
				.append(" AND M1.INBOUND_DATE <= TRUNC(SYSDATE) ")
				.append(" AND M1.INBOUND_NO = '").append(outboundNo).append("' ")
				.append(" AND M1.INBOUND_STATE='60' ") 
				.append(" AND ( M1.SEND_STATE = '00' OR M1.SEND_STATE IS NULL ) ");
				**/
				
				
				sb.append("SELECT														")
				.append("	'EDI_DC40','100','700','2','MBGMCR02','MBGMCR' 									")
				.append("	,'MBGMCR','ISE-TRN','LS','KRISE','SAPT05','LS','T05CLNT100'  							")
				.append("	,M1.CENTER_CD||M1.BRAND_CD||TO_CHAR(M1.ORDER_DATE,'YYYYMMDD')||M1.ORDER_NO||M2.LINE_NO ORDER_SEQ 		")
				.append("	,M1.FREE_VAL3 BRAND_DATE  											")
				.append("	,'20151218' INBOUND_DATE, M1.BRAND_NO  										")
				.append("	,M1.CENTER_CD||M1.BRAND_CD||TO_CHAR(M1.ORDER_DATE,'YYYYMMDD')||M1.ORDER_NO||M2.LINE_NO ORDER_SEQ2 		")
				.append("	,'01','101' 													")
				.append("	,M2.ORDER_QTY,'B',M1.BRAND_NO,M2.BRAND_LINE_NO   								")
				.append("FROM LI010NM M1 ,  LI010ND M2												")
				.append("WHERE  M1.CENTER_CD = M2.CENTER_CD AND M1.BRAND_CD = M2.BRAND_CD   							")
				.append("AND M1.ORDER_DATE = M2.ORDER_DATE   											")
				.append("AND M1.ORDER_NO = M2.ORDER_NO   											")
				.append("AND M1.CENTER_CD = 'A1'   												")
				.append("AND M1.BRAND_CD = '6101'   												")
				.append("AND M1.ORDER_DATE IN (TO_DATE('20151218', 'YYYYMMDD'),TO_DATE('20151219', 'YYYYMMDD'))  				")
				.append("AND M1.BRAND_NO <> '999999999'												");
				
			} else {
				sb.append(" SELECT 'EDI_DC40', '700', '2', ")
				.append(" 'SHP_OBDLV_CONFIRM_DECENTRAL01','SHP_OBDLV_CONFIRM_DECENTRAL','SHP_DB' ")
				.append(" 	   ,'ISE_TRN','LS','KRISE','SAPP01','LS','P01CLNT100' ")
				.append(" ,M1.CENTER_CD||M1.BRAND_CD||TO_CHAR(M1.OUTBOUND_DATE, 'YYYYMMDD')||M1.OUTBOUND_NO")
				.append(" ,M2.BRAND_NO,M2.BRAND_NO,M2.BRAND_NO,'X' ")
				.append(" ,M2.BRAND_NO,'WSHDRWADTI',TO_CHAR(M1.REG_DATETIME, 'YYYYMMDDHH24MISS') ")
				.append(" ,M2.BRAND_NO,M2.BRAND_LINE_NO,M2.ENTRY_QTY,M2.ENTRY_QTY,'1','1' ")
				.append(" ,M2.BRAND_NO,M2.BRAND_LINE_NO,'X' ")
				.append(" FROM LO020NM  M1, LO020ND M2 ")
				.append(" WHERE M1.CENTER_CD = M2.CENTER_CD ")
				.append(" AND M1.BRAND_CD = M2.BRAND_CD  ")
				.append(" AND M1.OUTBOUND_DATE = M2.OUTBOUND_DATE ")
				.append(" AND M1.OUTBOUND_NO = M2.OUTBOUND_NO ")
				.append(" AND M1.CENTER_CD = 'A1' ")
				.append(" AND M1.OUTBOUND_STATE = '50' ")
//				.append(" AND ( M1.FREE_VAL5 = '00' OR M1.FREE_VAL5 IS NULL ) ")
				.append(" AND ( M1.SEND_STATE = '00' OR M1.SEND_STATE IS NULL ) ")
				.append(" AND M1.BRAND_CD = '6101' ")
				.append(" AND M1.OUTBOUND_DATE <= TRUNC(SYSDATE) ")
				.append(" AND M1.OUTBOUND_NO = '").append(outboundNo).append("' ")
				.append(" ORDER BY M2.BRAND_NO, M2.BRAND_LINE_NO ");
			}
			
			pstmt = conn.prepareStatement(sb.toString());
			logger.info(sb.toString());
//			System.out.println(sb.toString());
			rs = pstmt.executeQuery();
			int rowLenght = rs.getMetaData().getColumnCount();
			while(rs.next()) {
				String[] row = new String[rowLenght];
				for(int i=1; i<=rowLenght; i++) {
					row[i-1] = rs.getString(i);
				}
				sendData.add(row);
			}
		} catch (SQLException e) {
			loggerErr.error(e.getMessage());
		} catch (Exception e) {
			loggerErr.error(e.getMessage());
		} finally {
			try {
				if(rs!=null) {
					rs.close();
				}
				if(pstmt!=null) {
					pstmt.close();
				}
				if(conn!=null) {
					conn.close();
				}
			} catch (Exception e) {
				loggerErr.error(e.getMessage());
			}
		}
		return sendData;
	}

	public boolean isOtherProcess() {
		return isOtherProcess;
	}

	public void setOtherProcess(boolean isOtherProcess) {
		this.isOtherProcess = isOtherProcess;
	}
	
	public void updateSucc(String orderSeq, String inoutCd) {
		
		Connection conn = null;
		ConnectionManager connMgr = ConnectionManager.getInstances();
		PreparedStatement pstmt = null;
		
		
		StringBuilder sb = new StringBuilder();
		if("D10".equals(inoutCd)) {
			logger.debug("updateSucc "+inoutCd+" orderSeq => "+orderSeq);
			sb.append(" UPDATE LO020NM ")
			.append(" SET SEND_STATE = '60', FREE_VAL5 = '60' ")
			.append(" WHERE (CENTER_CD||BRAND_CD||TO_CHAR(OUTBOUND_DATE,'YYYYMMDD')||OUTBOUND_NO) = ? ");
			
		} else {
			logger.debug("updateSucc "+inoutCd+" orderSeq => "+orderSeq.substring(0, 18));
			orderSeq = orderSeq.substring(0, 18);
			sb.append(" UPDATE LI020NM ")
			.append(" SET SEND_STATE = '60'")
			.append(" WHERE (CENTER_CD||BRAND_CD||TO_CHAR(INBOUND_DATE,'YYYYMMDD')||INBOUND_NO) = ? ");
		}
		
		try {
			logger.debug(sb.toString());
			conn = connMgr.connect("wmsdb");
			conn.setAutoCommit(false);
			pstmt = conn.prepareStatement(sb.toString());
			pstmt.setString(1, orderSeq);
			
			int result = pstmt.executeUpdate();
			logger.info("updateSucc was processed => "+result);
			
			conn.commit();
		} catch (SQLException e) {
			try {
				conn.rollback();
			} catch (Exception e1) {
				loggerErr.error(e.getMessage());
			}
			loggerErr.error(e.getMessage());
		} catch (Exception e) {
			try {
				conn.rollback();
			} catch (Exception e1) {
				loggerErr.error(e.getMessage());
			}
			loggerErr.error(e.getMessage());
		} finally {
			try {
				if(pstmt!=null) {
					pstmt.close();
				}
			} catch (SQLException e) {
				loggerErr.error(e.getMessage());
			} catch (Exception e1) {
				loggerErr.error(e1.getMessage());
			}
		}
	}
}
