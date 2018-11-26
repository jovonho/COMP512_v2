package Server.Common;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;

public class Log implements Serializable {
	protected String m_name;
	protected String fileName;
	
	public Log(String p_name) {
		m_name = p_name;
		fileName = "log//" + m_name + ".log";
	}
	
	public static Log getLog(String p_fileName) throws FileNotFoundException, Exception
	{
		FileInputStream fis = new FileInputStream(p_fileName);
		ObjectInputStream ois = new ObjectInputStream(fis);
		Log ret = (Log) ois.readObject();
		fis.close();
		ois.close();
		return ret;
	}
	
	public boolean flushLog(){
		try {
			FileOutputStream fos = new FileOutputStream(fileName);
			ObjectOutputStream oos = new ObjectOutputStream(fos);
			oos.writeObject(this);
			oos.close();
			fos.close();
			return true;
		} catch (IOException e) {
			e.printStackTrace();
			return false;
		}
	}
	
}
