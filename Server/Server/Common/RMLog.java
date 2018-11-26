package Server.Common;

import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.ObjectOutputStream;
import java.util.ArrayList;

public class RMLog extends Log {
	
	private ArrayList<Integer> transactions;
	

	public RMLog(String p_name) {
		super(p_name);
		
		transactions = new ArrayList<Integer>();
		
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

	public ArrayList<Integer> getTransactions() {
		return transactions;
	}
	
	public void updateLog(int xid) {
		transactions.add(xid);
	}
	
	public void removeTxLog(int xid){
		transactions.remove((Integer) xid);
	}
}
