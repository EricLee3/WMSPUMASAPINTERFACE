package com.gics.edi.common;

import java.io.File;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Properties;
import java.util.Vector;

import com.jcraft.jsch.Channel;
import com.jcraft.jsch.ChannelSftp;
import com.jcraft.jsch.JSch;
import com.jcraft.jsch.JSchException;
import com.jcraft.jsch.Session;
import com.jcraft.jsch.SftpException;
import com.jcraft.jsch.ChannelSftp.LsEntry;

public class SFTPManager {
	private Session session = null;
	private Channel channel = null;
	private ChannelSftp channelSftp = null;
	
	public void init(String host, String userName, String password, int port) {
        JSch jsch = new JSch();
        try {
			session = jsch.getSession(userName, host, port);
			session.setPassword(password);
			
			Properties config = new java.util.Properties();
			config.put("StrictHostKeyChecking", "no");
			session.setConfig(config);
			session.connect();
			
			channel = session.openChannel("sftp");
			channel.connect();
        } catch (JSchException e) {
            e.printStackTrace();
        }
       channelSftp = (ChannelSftp) channel;
    }
	
	public ArrayList<String> ls(String dir) {
		
		ArrayList<String> files = new ArrayList<String>();
		Vector v = null;
		try {
			v = channelSftp.ls(dir);
			if(v!=null) {
				for(int i=0; i<v.size(); i++) {
					Object obj = v.elementAt(i);
					if(obj instanceof LsEntry){ 
						files.add(((LsEntry)obj).getLongname());
					}
				}
			}
		} catch (SftpException e) {
			e.printStackTrace();
		} catch (Exception e) {
			e.printStackTrace();
		}
		return files;
	}
	
	
	public void upload(String dir, File file) {
		FileInputStream in = null;
		try {
			in = new FileInputStream(file);
			channelSftp.cd(dir);
			channelSftp.put(in, file.getName());
		} catch (SftpException e) {
			e.printStackTrace();
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} finally {
			try {
				in.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
	}
	
	public void deleteFile(String dir, String delFileName) throws IOException {
		

        try {
        	channelSftp.cd(dir);
        	
//            logger.debug("Will try to delete " + fileName);
            channelSftp.rm(delFileName);
            System.out.println("delete: "+dir+delFileName);
        }
        catch (SftpException e) {
            e.printStackTrace();
        }
    }


    public void download(String dir, String downloadFileName, String path) {
		InputStream in = null;
		FileOutputStream out = null;
		try {
			channelSftp.cd(dir);
			in = channelSftp.get(downloadFileName);
		} catch (SftpException e) {
			e.printStackTrace();
		} 
	
		try {
			out = new FileOutputStream(new File(path));
			int i;
			while ((i = in.read()) != -1) {
				out.write(i);
			}
		} catch (IOException e) {
			e.printStackTrace();
		} finally {
			try {
				out.close();
				in.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
	}

	public void disconnection() {
	    if(channelSftp != null){
	    	channelSftp.disconnect();
	    }
	    
	    if(channel != null){
	    	channel.disconnect();
	    }
	    
	    if(session != null){
	    	session.disconnect();
	    }
	}
}
