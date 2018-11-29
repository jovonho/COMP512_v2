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

public class TransactionManagerLog extends Log 
{
	protected int xidCounter;
	public int cxid = 0;
	public TwoPCstatus status = TwoPCstatus.None;
	private ArrayList<Integer> transactions;
	public int[] voteResponses = {-1, -1, -1, -1};
	private boolean usedCustomers = false;
	private  int customerVote = -1;
	private boolean crashModeEight = false;
	
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
			oos.close();
			fos.close();
		} 
		catch (FileNotFoundException e) {
			e.printStackTrace();
		}
		catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	public void setCustomerVote(int i) {
		customerVote = i;
		flushLog();
	}
	
	public int getCustomerVote() {
		return customerVote;
	}
	
	public void resetCustomerVote() {
		customerVote = -1;
		flushLog();
	}
	
	public void setUsedCustomer(boolean value) {
		usedCustomers = value;
		flushLog();
	}
	
	public boolean getUsedCustomer() {
		return usedCustomers;
	}
	public void setStatus(TwoPCstatus status) {
		this.status = status;
		flushLog();
	}
	
	public TwoPCstatus getStatus() {
		return status;
	}
	
	public void setCrashModeEight(){
		crashModeEight=true;
	}
	
	public void resetCrashModeEight(){
		crashModeEight=false;
		flushLog();
	}
	
	public boolean getCrashModeEight(){
		return crashModeEight;
	}
	
	
	public int getCounter() {
		return xidCounter;
	}
	
	public ArrayList<Integer> getTransactions() {
		return transactions;
	}
	
	public void updateLog(int xid) {
		xidCounter = xid;
		transactions.add(xid);
	}
	
	public void removeTxLog(int xid){
		transactions.remove((Integer) xid);
	}


	public void resetCustomer() {
		usedCustomers = false;
		flushLog();
	}
}
