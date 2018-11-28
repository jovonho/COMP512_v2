package Server.Common;

import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.ObjectOutputStream;
import java.util.ArrayList;

public class RMLog extends Log {
	
	private int xid;
	private Status status;
	/**
	 * -1 = none
	 * 0 = vote no
	 * 1 = vote yes
	 */
	private int response = 0;
	
	public RMLog(String p_name) {
		super(p_name);
		
		status = Status.None;
		
		FileOutputStream fos;
		try {
			fos = new FileOutputStream(fileName);
			ObjectOutputStream oos = new ObjectOutputStream(fos);
			oos.writeObject(this);
			fos.close();
			oos.close();
		} 
		catch (FileNotFoundException e) {
			e.printStackTrace();
		}
		catch (Exception e) {
			e.printStackTrace();
		}
	}

	public Status getStatus() {
		return status;
	}
	
	public int getXid(){
		return xid;
	}
	
	public void updateStatus(Status new_status, int xid) {
		status = new_status;
		this.xid=xid;
		flushLog();
	}

	public void clearStatus() {
		status = Status.None;
		this.xid=0;
		flushLog();
	}
}
