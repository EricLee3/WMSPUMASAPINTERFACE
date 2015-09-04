package com.gics.servlet;

import java.util.Properties;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import com.gics.edi.common.PropManager;

public class JSonApi extends HttpServlet {
	private static final long serialVersionUID = 1L;
	public void doPost(HttpServletRequest request, HttpServletResponse response)
		throws ServletException, java.io.IOException {
		String fork = request.getParameter("fork");
		if(fork == null) {
			return;
		}
		JSONObject jObj = new JSONObject();
		PropManager propMng = PropManager.getInstances();
		Properties prop = propMng.getProp(fork);
		try {
			JSONArray rowsJArr = new JSONArray();
			String[] keyList = prop.getProperty("prop.key").split(",");
			int keyIdx = 1;
			for(int depthIndex=keyList.length; depthIndex>=1; depthIndex--) {
				String type = prop.getProperty("prop."+keyList[depthIndex-1]+".type");
				JSONObject rowJObj = new JSONObject();
				int i=0;
				
				while(prop.getProperty("prop."+keyList[depthIndex-1]+".id"+i)!= null) {
					if(type.equals("data")) {
						rowJObj.put(prop.getProperty("prop."+keyList[depthIndex-1]+".id"+i).trim()
								,prop.getProperty("prop."+keyList[depthIndex-1]+".value"+i).trim());
					} else {
						rowJObj.put(prop.getProperty("prop."+keyList[depthIndex-1]+".id"+i).trim()
								, request.getParameter("col"+i));
					}
					i++;
				}
				rowsJArr.add(rowJObj); 
				jObj.put(prop.getProperty("prop.key").split(",")[keyList.length-keyIdx], rowsJArr);
				System.out.println("jObj: "+jObj);
				keyIdx++;
			}
		} catch (Exception e) {
			e.printStackTrace();
		}		
	}
	public void doGet(HttpServletRequest request, HttpServletResponse response)
		throws ServletException, java.io.IOException {
		doPost(request, response);
	} 
}
