package com.gics.edi.puma.main;

import java.io.File;

import org.w3c.dom.Document;
import org.w3c.dom.NodeList;

import com.gics.edi.common.XMLManager;
import com.gics.edi.linker.Linker;

public class PumaTest {

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		
		File f = new File("D:/articleMasterTest/");
		String[] fileName = f.list();
//		XMLManager xmg = new XMLManager();
		
		for(int i=0; i<fileName.length; i++) {
//			Document doc = xmg.read("D:/articleMasterTest/"+fileName[i]+".xml");
			Linker linker = new Linker();
			linker.createKeyTypeData("recvItem", "D:/articleMasterTest/"+fileName[i]);
			System.out.println(fileName[i]);
		}
		
		
		
//		XMLManager xmg = new XMLManager();
//		xmg.read("D:/O_DESADV_1503_0000000000195171.xml");
	}
}
