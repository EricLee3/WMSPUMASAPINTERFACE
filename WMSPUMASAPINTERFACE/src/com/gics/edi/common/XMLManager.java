package com.gics.edi.common;

import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.InputStreamReader;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;

import org.w3c.dom.Document;

import com.sun.org.apache.xml.internal.serialize.OutputFormat;
import com.sun.org.apache.xml.internal.serialize.XMLSerializer;

public class XMLManager {
	
	public Document read(String path) {
	
		DocumentBuilderFactory factory = null;
		DocumentBuilder docBuilder = null;
		Document doc = null;
		
		ByteArrayInputStream bis = null;
		FileInputStream fis = null;
		BufferedReader br = null;
		StringBuilder xml = new StringBuilder();
		try {
			fis = new FileInputStream(new File(path));
			br = new BufferedReader(new InputStreamReader(fis, "UTF-8"));
			String line = "";
			while ((line=br.readLine()) != null) {
				xml.append(line);
			}
//		System.out.println(xml.toString());
			
			bis = new ByteArrayInputStream(xml.toString().getBytes("UTF-8"));
			factory = DocumentBuilderFactory.newInstance();
			docBuilder = factory.newDocumentBuilder();
			doc = docBuilder.parse(bis);
			doc.getDocumentElement().normalize();
		} catch (Exception e) {
			e.printStackTrace();
		}
		return doc;
	}
	
	public Document createXmlDoc(String xml) {
		
		DocumentBuilderFactory factory = null;
		DocumentBuilder docBuilder = null;
		Document doc = null;		
		ByteArrayInputStream bis = null;
		try {
			bis = new ByteArrayInputStream(xml.toString().getBytes("UTF-8"));
			factory = DocumentBuilderFactory.newInstance();
			docBuilder = factory.newDocumentBuilder();
			doc = docBuilder.parse(bis);
			doc.getDocumentElement().normalize();
		} catch (Exception e) {
			e.printStackTrace();
		}
		return doc;
	}
	
	public void write(Document doc, String path){
		FileOutputStream fos = null;
		try {
			OutputFormat of = new OutputFormat();
			of.setEncoding("UTF-8");
			of.setOmitXMLDeclaration(false);
			of.setIndenting(true);
			of.setIndent(4);
			
			fos = new FileOutputStream(new File(path));
			XMLSerializer serializer = new XMLSerializer(fos, of);
			serializer.serialize(doc);
			
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			try {
				fos.close();
			} catch (Exception e) {
				e.printStackTrace();				
			}
		}
	}
}
