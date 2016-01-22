package com.gics.edi.common;

import java.io.FileInputStream;
import java.util.Properties;

public class PropManager {
	
	private Properties prop;
	private static PropManager propManager; 
	private PropManager() {}
	public static PropManager getInstances() {
		if(propManager == null) {
			propManager = new PropManager();
		}
		return propManager;
	}
	public Properties getProp(String propName) {
		try {
			prop = new Properties();
//			FileInputStream fis = new FileInputStream("D:\\GITREPOSITORY\\WMSPUMASAPINTERFACE\\WMSPUMASAPINTERFACE\\props\\"+propName+".properties");
			FileInputStream fis = new FileInputStream("/export/home/wms/WMSLinker/props/"+propName+".properties");
			prop.load(fis);
			
		} catch (Exception e) {
			e.printStackTrace();
		}
		return prop;
	}
	
	public Properties getProp(String propName, String type) {
		try {
			prop = new Properties();
			FileInputStream fis = null;
			
			if(type.length() > 0) {				
//				fis = new FileInputStream("D:\\GITREPOSITORY\\WMSPUMASAPINTERFACE\\WMSPUMASAPINTERFACE\\props\\"+type+"\\"+propName+".properties");
				fis = new FileInputStream("/export/home/wms/WMSLinker/props/"+type+"/"+propName+".properties");
			} else {
//				fis = new FileInputStream("D:\\GITREPOSITORY\\WMSPUMASAPINTERFACE\\WMSPUMASAPINTERFACE\\props\\"+propName+".properties");
				fis = new FileInputStream("/export/home/wms/WMSLinker/props/"+propName+".properties");
			}
			prop.load(fis);
			
		} catch (Exception e) {
			e.printStackTrace();
		}
		return prop;
	}
}
