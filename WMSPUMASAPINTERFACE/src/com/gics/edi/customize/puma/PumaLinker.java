package com.gics.edi.customize.puma;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;

import org.apache.log4j.Logger;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.NodeList;

import com.gics.edi.common.XMLManager;
import com.gics.edi.conn.ConnectionManager;

public class PumaLinker {
	private static Logger loggerErr = Logger.getLogger("process.err");
	private static Logger logger = Logger.getLogger("process.puma");
	
	public void updateItemStyle(String itemCd, String strSize) {
		logger.debug("itemStyle update start ");
		StringBuilder sb = new StringBuilder();
		String itemSize = strSize;
		if(strSize.contains(",")) {
			itemSize = strSize.split(",")[1].trim();
		}
		ConnectionManager connManager = ConnectionManager.getInstances();
		Connection conn = null;
		PreparedStatement pstmt = null;
				
		try {
			logger.debug("itemCd: "+itemCd);
			logger.debug("itemSize: "+itemSize);
//			logger.debug("type: "+type);
			
			sb.append("	UPDATE CMITEM ")
			  .append(" SET ITEM_SIZE = '").append(itemSize).append("'")
			  .append(" WHERE BRAND_CD = '6101'") 
			  .append(" AND ITEM_CD = '").append(itemCd).append("'");
			
			
//			if("krsize".equals(type)) {
//				sb.append("	UPDATE CMITEM ")
//				  .append(" SET ITEM_SIZE = '").append(itemSize).append("'")
//				  .append(" WHERE BRAND_CD = '6101'") 
//				  .append(" AND ITEM_CD = '").append(itemCd).append("'");
//			} else {
//				sb.append("	UPDATE CMITEM ")
//				  .append(" SET ITEM_NM = REPLACE(ITEM_NM, '^',' '), ITEM_SIZE = '").append(itemSize).append("'")
//				  .append(" WHERE BRAND_CD = '6101'") 
//				  .append(" AND ITEM_CD = '").append(itemCd).append("'");
//			}
						
			logger.debug(sb.toString());
//			System.out.println(sb.toString());
			conn = connManager.connect("wmsdb");
			pstmt = conn.prepareStatement(sb.toString());
			int result = pstmt.executeUpdate();
			sb.delete(0, sb.length());
			if(result>0) {
				logger.info("itemStyle update succ!");
				conn.commit();
			}
			logger.debug("완료");
		} catch (SQLException e) {
			loggerErr.error(e.getMessage());
			e.printStackTrace();
		} catch (Exception e) {
			loggerErr.error(e.getMessage());
			e.printStackTrace();
		} finally {
			try {
				if(pstmt!=null) {
					pstmt.close();
				}
			} catch (Exception e) {
				loggerErr.error(e.getMessage());
				e.printStackTrace();
			}
		}
	}

	public void updateKRSize(String path) {
//		path = "D:\\eclipse\\projects\\WMSLinker\\files\\recv\\O_ARTMAS_1505_0000000025901769.xml";
		
		XMLManager xmlMgr = new XMLManager();
		Document doc = xmlMgr.read(path);
		String krCd = "";
		NodeList nodeList = doc.getElementsByTagName("E1BPE1MATHEAD");
		ArrayList<String[]> list = new ArrayList<String[]>();
		ArrayList<String[]> result = new ArrayList<String[]>();
		
		for(int i=0; i<nodeList.getLength(); i++) {
			Element element = (Element) nodeList.item(i);
			if(element != null) {
				krCd = getChildren(element, "CHAR_PROF");
				if(krCd.length() > 0) {
					break;
				}
			}
		}
		nodeList = doc.getElementsByTagName("E1BPE1MARAEXTRT");
		for(int i=0; i<nodeList.getLength(); i++) {
			String[] row = new String[3];
			Element element = (Element) nodeList.item(i);
			if(element != null) {
				row[0] = getChildren(element, "MATERIAL");
			}
			if(element != null) {
				row[1] = getChildren(element, "FIELD1");
			}
			if(element != null) {
				row[2] = getChildren(element, "FIELD4");
			}
			list.add(row);
		}
		if(krCd.length() > 0) {
			for(int i=0; i<list.size(); i++) {
				if(krCd.equals(list.get(i)[1]) 
					&& list.get(i)[2].length() > 1 ) {
					result.add(list.get(i));
				}
			}
		}
		
		if(result.size() > 0) {
			for(int i=0; i<result.size(); i++) {
				for(int j=0; j<result.get(0).length; j++) {
					logger.debug(result.get(i)[j]+ " , ");
//					System.out.println(result.get(i)[j]+ " , ");
				}
				logger.debug("\n");
//				System.out.print("\n");
			}
			for(int i=0; i<result.size(); i++) {
				try {
					updateItemStyle(result.get(i)[0],String.valueOf((int)(Float.parseFloat(result.get(i)[2])*10)));
				} catch (Exception e) {
					logger.debug(e.getMessage());
					logger.debug(e.toString());
				}
			}
		}
	}
	
	public String getChildren(Element element, String tagName) {
		NodeList list = element.getElementsByTagName(tagName);
		Element cElement = (Element) list.item(0);
	  
		if(cElement==null) {
			return "";
		}
		
		if(cElement.getFirstChild()!=null) {
			return cElement.getFirstChild().getNodeValue(); 
		} else {
			return "";
		}
	}
	
//	public static void main(String[] args) {
//		PumaLinker pumaLinker = new PumaLinker();
//		pumaLinker.updateKRSize("");
//		
//	}
}
