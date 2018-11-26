package Server.Common;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutput;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.util.ArrayList;

public class TransactionManagerLog extends Log {

	protected int xidCounter;
	private ArrayList<Integer> transactions;
	
	public TransactionManagerLog(String p_name)
	{
		super(p_name);
		
		// Check if log exists on disk, if it doesn't initialize an empty else 
		xidCounter = 0;
		transactions = new ArrayList<Integer>();
		
		FileOutputStream fos;
		try {
			fos = new FileOutputStream(fileName);
			ObjectOutputStream oos = new ObjectOutputStream(fos);
			oos.writeObject(this);
			System.out.println("In Log constructor writing: " + transactions.toString());
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
	
	public int getCounter() {
		return xidCounter;
	}
	
	public ArrayList<Integer> getTransactions() {
		return transactions;
	}
	
	public void updateLog(int xid) {
		System.out.println("From Log (b4): " + transactions.toString());
		xidCounter = xid;
		transactions.add(xid);
		System.out.println("From Log: " + transactions.toString());
	}
	
	public void removeTxLog(int xid){
		transactions.remove((Integer) xid);
	}
	
	@Override
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
