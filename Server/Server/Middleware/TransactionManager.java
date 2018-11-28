package Server.Middleware;

import Server.Interface.*;

import java.rmi.NotBoundException;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;

import Server.Common.*;
import Server.LockManager.*;
import Server.Middleware.TransactionManager;

import java.util.*;
import java.rmi.RemoteException;
import java.io.*;

public class TransactionManager {	
	
	/**
	 * A simple way to keep track of the next transaction ID we will use.
	 */
	private int xidCounter;
	/**
	 * Time to live.
	 */
	private final int TTL = 40000;
	/**
	 * The time in ms that we wait for a crashed resource manager to recover.
	 */
	final private int recoveryCheckInterval = 10000;
	/**
	 * The lock manager of the system. Handles all data concurrency control for all RMs
	 */
	private LockManager lockManager = new LockManager();

	/**
	 * A list of transactions that have been started but not yet commited/aborted.
	 */
	private ArrayList<Integer> activeTransactions;
	/**
	 * A list of aborted transactions. Silently aborted transactions are added here.
	 */
	private ArrayList<Integer> abortedTransactions = new ArrayList<Integer>();

	/**
	 * Our committed data stored on RAM.
	 */
	private RMHashMap m_dataCopy = new RMHashMap();
	/**
	 * Our local data stored on RAM.
	 */
	private Map<Integer, ArrayList<String>> transactionMap = new HashMap<>();
	/**
	 * Holds the keys of items that a transaction deleted. The items are actually deleted from storage at commit time.
	 */
	private Map<Integer, ArrayList<String>> toDeleteMap = new HashMap<>();
	
	/**
	 * A series of timers ensuring that the transaction fails if the client takes too long.
	 */
	private Map<Integer, Timer> transactionTimers = new HashMap<Integer, Timer>();
	
	/**
	 * An association to the middleware. Middleware is a local object, no RMI involved.
	 */
	private Middleware customersManager = null;

	/**
	 * A long that maintains a set of transactions which we have 
	 */
	private TransactionManagerLog log;
	
	/**
	 * Holds all transactions that were active last time the server was active.
	 */
	private ArrayList<Integer> crashedTransactions = new ArrayList<Integer>();
	
	private RMHashMap temp;
	
	private int crashMode = 0;
	
	static public int vote = -1;

	public TransactionManager(Middleware custs)
	{
		try 
		{
			log = (TransactionManagerLog) TransactionManagerLog.getLog("log//transactionManager.log");
		} 
		catch (FileNotFoundException e) 
		{
			log = new TransactionManagerLog("transactionManager");
		}
		catch (Exception e) 
		{
			e.printStackTrace();
		}
		
		// fetch data from log
		xidCounter = log.getCounter();
		crashedTransactions = (ArrayList<Integer>) log.getTransactions().clone();
		
		for(int id : crashedTransactions){
			System.out.println(Color.red + "TM::Transaction " + id + " was still active before crash." + Color.reset);
		}
		
		
		// reset activeTransactions upon starting 
		activeTransactions = new ArrayList<>();
		
		this.customersManager = custs;
				
	}
	
	
	public void abortCrashedTx() throws RemoteException, InvalidTransactionException {
		for (int id : crashedTransactions) {
			try {
				abortCrashedTx(id);
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
		crashedTransactions = new ArrayList<>();
	}
	
	// Crashed transactions have their xid in the activeTrasnactions list but don't have a timer
	private void abortCrashedTx(int xid) throws RemoteException, InvalidTransactionException {
		
		Middleware.m_flightsManager.abortCrashedTx(xid);
		Middleware.m_carsManager.abortCrashedTx(xid);
		Middleware.m_roomsManager.abortCrashedTx(xid);
		
		System.out.println(Color.yellow + "Transaction "  + xid + " aborted." + Color.reset);
		
		log.removeTxLog(xid);
		log.flushLog();
	}
	

	// Updated - Milestone 3
	/**
	 * Transactions now exist at each resource manager. They will thus have an associated transaction map and to delete map at each RM, even if
	 * they don't actually access any items from these RMs.
	 * 
	 */
	public int start() throws RemoteException 
	{	
		cancelRMTimers();
		
		int xid = ++xidCounter;

		
		activeTransactions.add(xid);
		
		transactionMap.put(xid, new ArrayList<String>());
		toDeleteMap.put(xid, new ArrayList<String>());
		
		Timer timer = new Timer();
		timer.schedule(new TimerTask() {
			@Override
			public void run() {
				Trace.info("Transaction " + xid + " timed out");
				try {
					abort(xid);
				} catch (InvalidTransactionException e) {
					e.printStackTrace();
				} catch (RemoteException e) {
					e.printStackTrace();
				}
				abortedTransactions.add(xid);
			}
		}, TTL);
		
		transactionTimers.put(xid, timer);
		
		try {
			// NEW : We now call start on each resource manager
			Middleware.m_flightsManager.start(xid);
			Middleware.m_carsManager.start(xid);
			Middleware.m_roomsManager.start(xid);			
		}
		catch (RemoteException e) {
			System.out.println(Color.yellow + " Start RemoteException caught" + Color.reset);
		}
		// See not in  abort
		
		System.out.println("In TM b4 calling updateLog" + log.getTransactions().toString());
		log.updateLog(xid);
		System.out.println("In TM" + log.getTransactions().toString());
		log.flushLog();
		
		
		System.out.println("TM::transactions at end of start(): " + activeTransactions.toString());

		
		Trace.info("TM::start() Transaction " + xid + " started");
		resetRMTimers();
		return xid;
	}
	
	
	
	//crash APE
	public void resetCrashes() throws RemoteException{
		cancelRMTimers();

		crashMode = 0;

		Middleware.m_flightsManager.resetCrashes();
		Middleware.m_carsManager.resetCrashes();
		Middleware.m_roomsManager.resetCrashes();

		Trace.info("Crash modes at the middleware and the resource manager reset to: " + crashMode);
		resetRMTimers();
	}

	public void crashMiddleware(int mode) throws RemoteException{
		cancelRMTimers();
		crashMode=mode;

		Trace.info("Crash mode at the middleware set to: " + crashMode);
		resetRMTimers();
	}

	public void crashResourceManager(String name, int mode) throws RemoteException{
		cancelRMTimers();

		if(name.equals("flights")){
			Middleware.m_flightsManager.crashResourceManager(name, mode);
			Trace.info("Setting crash mode at the Rm: "+name+", to: " + mode);
		}
		else if(name.equals("cars")){
			Middleware.m_carsManager.crashResourceManager(name, mode);
			Trace.info("Setting crash mode at the Rm: "+name+", to: " + mode);
		}
		else if(name.equals("rooms")){
			Middleware.m_roomsManager.crashResourceManager(name, mode);
			Trace.info("Setting crash mode at the Rm: "+name+", to: " + mode);
		}

		resetRMTimers();

	}
	
	private void resetRMTimers() throws RemoteException
	{
		try {
			Middleware.m_flightsManager.resetTimer();
		}
		catch (Exception e) {
			System.out.println(Color.red  + " resetTimer remote exception caught. Flights down" + Color.reset);
		}
		try {
			Middleware.m_carsManager.resetTimer();
		}
		catch (Exception e) {
			System.out.println(Color.red  + " resetTimer remote exception caught. Cars down" + Color.reset);
		}
		try {
			Middleware.m_roomsManager.resetTimer();
		}
		catch (Exception e) {
			System.out.println(Color.red  + " resetTimer remote exception caught. Rooms down" + Color.reset);
		}
		try {
			customersManager.resetTimer();
		}
		catch (Exception e) {
			System.out.println(Color.red  + " resetTimer remote exception caught. Customers down" + Color.reset);
		}
		
	}
	
	private void cancelRMTimers() throws RemoteException
	{
		try {
			Middleware.m_flightsManager.cancelTimer();
		}
		catch (Exception e) {
			System.out.println(Color.red  + " resetTimer remote exception caught. Flights down" + Color.reset);
		}
		try {
			Middleware.m_carsManager.cancelTimer();
		}
		catch (Exception e) {
			System.out.println(Color.red  + " resetTimer remote exception caught. Cars down" + Color.reset);
		}
		try {
			Middleware.m_roomsManager.cancelTimer();
		}
		catch (Exception e) {
			System.out.println(Color.red  + " resetTimer remote exception caught. Rooms down" + Color.reset);
		}
		try {
			customersManager.cancelTimer();
		}
		catch (Exception e) {
			System.out.println(Color.red  + " resetTimer remote exception caught. Customers down" + Color.reset);
		}
	}
	
	// Updated - Milestone 3
	/**
	 * TODO: Problems with Middleware because it doesn't extend RM class it individually implements IResouceManager so we need
	 * to copy paste these functions in the Middleware.
	 * 
	 * 
	 * TODO: What about the aborted transactions list?
	 * 
	 * 
	 * We now need to abort a transaction at each RM.
	 * Abort could be called by client or by an expired timer.
	 */
	public void abort(int xid) throws InvalidTransactionException, RemoteException 
	{
		if (transactionTimers.containsKey(xid)) {
			cancelTimer(xid);			
		}
		cancelRMTimers();
		
		if (!activeTransactions.contains(xid)) 
		{
			throw new InvalidTransactionException(xid, null);
		}
		else
		{

//			transactionTimers.get(xid).cancel();
			transactionTimers.remove(xid);
			
			try {
				Middleware.m_flightsManager.abort(xid);
			}
			catch (Exception e){
				System.out.println(Color.yellow + " Abort: Remote Exception caught. Flights down." + Color.reset);
			}
			try {
				Middleware.m_carsManager.abort(xid);
			}
			catch (Exception e){
				System.out.println(Color.yellow + " Abort: Remote Exception caught. Cars down." + Color.reset);
			}
			try {
				Middleware.m_roomsManager.abort(xid);				
			}
			catch (Exception e){
				System.out.println(Color.yellow + " Abort: Remote Exception caught. Rooms down." + Color.reset);
			}
			
			
			
			if (transactionMap.containsKey(xid)) {
				for (String key : transactionMap.get(xid)) 
				{
					removeDataCopy(xid, key);
				}				
			}
			
			abortedTransactions.add(xid);
			activeTransactions.remove((Integer) xid);
			
			transactionMap.remove(xid);
			toDeleteMap.remove(xid);
			lockManager.UnlockAll(xid);
			
			System.out.println("From TM: " + xid + " ABORTED");
		}
		
		log.removeTxLog(xid);
		log.flushLog();
		
		resetRMTimers();
	}



	public boolean shutdown() throws RemoteException{
		return false;
	}
	
	// transactionManager holds localCopy of customers so it will coordinate a prepare with the middleware whihc holds the storage version of customers
	private boolean prepare(int xid) throws InvalidTransactionException, TransactionAbortedException, RemoteException {
		
		checkValid(xid);
		
		System.out.println(Color.cyan + "2PC::Customers -- Prepare Initiated " + Color.reset);
		
		if (!activeTransactions.contains((Integer) xid)) 
		{	
			System.out.println(Color.red + "2PC::Customers -- Prepare FAILED" + Color.reset);
			
			vote = 0;
			return false;
		}
		
		// Writing the latest commit version of the data to temp -- "committing" to temp
		// Getting the storage version of data from Middleware -- architecture shitting the bed
		temp = (RMHashMap) customersManager.getMapClone();
		
		// Writing local copy to temp -- merging local copy with storage
		for (String key : transactionMap.get(xid)) 
		{
			RMItem toCommit = readDataCopy(xid, key);
			if (toCommit == null) {
				break;
			}
			else {
				temp.put(key, toCommit);
			}
			removeDataCopy(xid, key);
		}
		
		transactionMap.remove((Integer) xid);
		
		for (String key : toDeleteMap.get(xid)) {
			if (key == null) {
				break;
			}
			else {
				temp.remove(key);
			}
		}
		toDeleteMap.remove((Integer) xid);

		// Writing the latest committed version to disk
		customersManager.storeMapPersistentNoSwap(temp);
		
		System.out.println(Color.cyan + "2PC::Customers -- Prepare SUCCESS" + Color.reset);
		
		vote = 1;
		return true;
	}

	
	public boolean commit(int xid) throws RemoteException, InvalidTransactionException, TransactionAbortedException
	{
		checkValid(xid);
		cancelTimer(xid);
		cancelRMTimers();
		
		
		System.out.println(Color.blue + "TM::2PC Protocol initiated." + Color.reset);
	
		
		//crash API
		if(crashMode==1){
			System.out.println("Crash for mode: " + crashMode);
			System.exit(1);
		}
		

		
		
		int counter = 0;
		
		while(counter < 3) {
			try
			{
				System.out.println(Color.yellow + "2PC::Calling prepare on Flights" + Color.reset);
				Middleware.m_flightsManager.prepare(xid);	
				System.out.println(Color.cyan + "2PC::Flights prepare went through " + Color.reset);
				break;
			}
			catch (Exception e)
			{
				System.out.println(Color.red + "2PC::Prepare - flightsManager is down. Attempt " + counter  + Color.reset);
				
				try 
				{
					Thread.sleep(recoveryCheckInterval);
					System.out.println(Color.yellow + "Attempting to rebind Flights" + Color.reset);
					
					//Manually re-lookup the RM from the registry
					Registry registry = LocateRegistry.getRegistry("lab2-4.cs.mcgill.ca", 1099);
					Middleware.m_flightsManager = (IResourceManager) registry.lookup("group33Flights");
					
				} 
				catch (Exception e1) 
				{
					System.out.println(Color.red + "Rebind failed" + Color.reset);
					e1.printStackTrace();
				}
				counter++;
			}
		}
		
		if (counter == 3) {
			System.out.println(Color.red + "2PC::Failed to call prepare on Flights -- ABORT " + Color.reset);
			abort(xid);	
			return false;
		}
		
		counter = 0;
		
		while(counter < 3) {
			try
			{
				System.out.println(Color.yellow + "2PC::Calling prepare on Cars" + Color.reset);
				Middleware.m_carsManager.prepare(xid);	
				System.out.println(Color.cyan + "2PC::Cars prepare went through " + Color.reset);
				break;
			}
			catch (Exception e)
			{
				System.out.println(Color.red + "2PC::Prepare - carsManager is down. Attempt " + counter  + Color.reset);
				
				try 
				{
					Thread.sleep(recoveryCheckInterval);
					System.out.println(Color.yellow + "Attempting to rebind Cars" + Color.reset);
					
					//Manually re-lookup the RM from the registry
					Registry registry = LocateRegistry.getRegistry("lab1-10.cs.mcgill.ca", 1099);
					Middleware.m_carsManager = (IResourceManager) registry.lookup("group33Cars");
					
				} 
				catch (Exception e1) 
				{
					System.out.println(Color.red + "Rebind failed" + Color.reset);
					e1.printStackTrace();
				}
				counter++;
			}
		}
		
		if (counter == 3) {
			System.out.println(Color.red + "2PC::Failed to call prepare on Cars -- ABORT " + Color.reset);
			abort(xid);	
			return false;
		}
		
		counter = 0;
		while(counter < 3) {
			try
			{
				System.out.println(Color.yellow + "2PC::Calling prepare on Rooms" + Color.reset);
				Middleware.m_roomsManager.prepare(xid);	
				System.out.println(Color.cyan + "2PC::Rooms prepare went through " + Color.reset);
				break;
			}
			catch (Exception e)
			{
				System.out.println(Color.red + "2PC::Prepare - roomsManager is down. Attempt " + counter  + Color.reset);
				
				try 
				{
					Thread.sleep(recoveryCheckInterval);
					System.out.println(Color.yellow + "Attempting to rebind Rooms" + Color.reset);
					
					//Manually re-lookup the RM from the registry
					Registry registry = LocateRegistry.getRegistry("open-4.cs.mcgill.ca", 1099);
					Middleware.m_roomsManager = (IResourceManager) registry.lookup("group33Rooms");
					
				} 
				catch (Exception e1) 
				{
					System.out.println(Color.red + "Rebind failed" + Color.reset);
					e1.printStackTrace();
				}
				counter++;
			}
		}
		
		if (counter == 3) {
			System.out.println(Color.red + "2PC::Failed to call prepare on Rooms -- ABORT " + Color.reset);
			abort(xid);	
			return false;
		}
		
		
		System.out.println(Color.yellow + "2PC::Calling prepare on Customers" + Color.reset);
		//Calling prepare on the "customers Manager"
		prepare(xid);
		System.out.println(Color.cyan + "2PC::Customers prepare went through " + Color.reset);
		
		
		
		
		//crash API
		if(crashMode==2)
		{
			System.out.println("Crash for mode: " + crashMode);
			System.exit(1);
		}
		
		
		
		// Creates a timer to wait for votes 
		Timer votesTimer = new Timer();
		votesTimer.schedule(new TimerTask() {
			@Override
			public void run() {
				Trace.info(Color.red + "Transaction " + xid + " timed out" + Color.reset);
				try {
					abort(xid);
				} 
				catch (Exception e) {
					e.printStackTrace();
				}
				abortedTransactions.add(xid);
			}
		}, 30000);
		
		long spinTime = 10000;
		int consensus = 0;
		

		counter = 0;
		while(counter < 3) {
			
			try 
			{
				System.out.println(Color.yellow + "2PC::Checking for Flights vote" + Color.reset);
				
				// I think if the RM is up but hasn't changed its vote, we will stay in this loop until the votesTimer expires
				if (Middleware.m_flightsManager.getVote() != -1) 
				{
					consensus += Middleware.m_flightsManager.getVote();	
					System.out.println(Color.cyan + "2PC::Flights vote received" + Color.reset);
					counter = 0;
					break;
				}
				
			}
			catch (Exception e) 
			{
				System.out.println(Color.red + "2PC::Can't get Flights vote. Attempt " + counter + Color.reset);
				
				try 
				{
					Thread.sleep(spinTime);
					System.out.println(Color.yellow + "Attempting to rebind Flights" + Color.reset);
					
					//Manually re-lookup the RM from the registry
					Registry registry = LocateRegistry.getRegistry("lab2-4.cs.mcgill.ca", 1099);
					Middleware.m_flightsManager = (IResourceManager) registry.lookup("group33Flights");
					
				} 
				catch (Exception e1) 
				{
					System.out.println(Color.red + "Rebind failed" + Color.reset);
					e1.printStackTrace();
				}
				counter++;
			}
		}
		if (counter == 3) 
		{
			System.out.println(Color.red + "2PC::Failed to receive Flights vote -- ABORT " + Color.reset);
			abort(xid);	
			return false;
		}
		
		
		//crash API
		if(crashMode==3)
		{
			System.out.println("Crash for mode: " + crashMode);
			System.exit(1);
		}
				
		
		counter = 0;
		while(counter < 3) {
			
			try 
			{
				System.out.println(Color.yellow + "2PC::Checking for Cars vote" + Color.reset);
				
				// I think if the RM is up but hasn't changed its vote, we will stay in this loop until the votesTimer expires
				if (Middleware.m_carsManager.getVote() != -1) 
				{
					consensus += Middleware.m_carsManager.getVote();	
					System.out.println(Color.cyan + "2PC::Cars vote received" + Color.reset);
					counter = 0;
					break;
				}
				
			}
			catch (Exception e) 
			{
				System.out.println(Color.red + "2PC::Can't get Cars vote. Attempt " + counter + Color.reset);
				
				try 
				{
					Thread.sleep(spinTime);
					System.out.println(Color.yellow + "Attempting to rebind Cars" + Color.reset);
					
					//Manually re-lookup the RM from the registry
					Registry registry = LocateRegistry.getRegistry("lab1-10.cs.mcgill.ca", 1099);
					Middleware.m_carsManager = (IResourceManager) registry.lookup("group33Cars");
					
				} 
				catch (Exception e1) 
				{
					System.out.println(Color.red + "Rebind failed" + Color.reset);
					e1.printStackTrace();
				}
				counter++;
			}
		}
		if (counter == 3) 
		{
			System.out.println(Color.red + "2PC::Failed to receive Cars vote -- ABORT " + Color.reset);
			abort(xid);	
			return false;
		}
		
		
		counter = 0;
		while(counter < 3) {
			
			try 
			{
				System.out.println(Color.yellow + "2PC::Checking for Rooms vote" + Color.reset);
				
				// I think if the RM is up but hasn't changed its vote, we will stay in this loop until the votesTimer expires
				if (Middleware.m_roomsManager.getVote() != -1) 
				{
					consensus += Middleware.m_roomsManager.getVote();	
					System.out.println(Color.cyan + "2PC::Rooms vote received" + Color.reset);

					counter = 0;
					break;
				}
				
			}
			catch (Exception e) 
			{
				System.out.println(Color.red + "2PC::Can't get Rooms vote. Attempt " + counter + Color.reset);
				
				try 
				{
					Thread.sleep(spinTime);
					System.out.println(Color.yellow + "Attempting to rebind Rooms" + Color.reset);
					
					//Manually re-lookup the RM from the registry
					Registry registry = LocateRegistry.getRegistry("open-4.cs.mcgill.ca", 1099);
					Middleware.m_roomsManager = (IResourceManager) registry.lookup("group33Rooms");
					
				} 
				catch (Exception e1) 
				{
					System.out.println(Color.red + "Rebind failed" + Color.reset);
					e1.printStackTrace();
				}
				counter++;
			}
		}
		if (counter == 3) 
		{
			System.out.println(Color.red + "2PC::Failed to receive Rooms vote -- ABORT " + Color.reset);
			abort(xid);	
			return false;
		}
		
		
		//Vote for Customers is DE FACTO set by calling prepare earlier. 
		System.out.println(Color.yellow + "Checking for Customers vote" + Color.reset);
		consensus += vote;
		System.out.println(Color.cyan + "2PC::Customers vote received" + Color.reset);


		votesTimer.cancel();
		
		//crash API
		if(crashMode==5)
		{
			System.out.println("Crash for mode: " + crashMode);
			System.exit(1);
		}
		
		
		
		if(consensus != 4)
		{
			System.out.println(Color.red + "2PC::Consensus was not reached: " + consensus + "/4 Only" + Color.reset);
			System.out.println(Color.red + "2PC::ABORT" + Color.reset);
			abort(xid);
			return false;
		}
		
		
		System.out.println(Color.cyan + "2PC::Consensus reached -- WE ARE COMMITING" + Color.reset);
		
		//Possibly Write COMMIT log ???
		

		
		counter = 0;
		while(counter < 3) {
			try
			{
				System.out.println(Color.yellow + "2PC::Sending COMMIT to Flights Manager" + Color.reset);
				Middleware.m_flightsManager.commit(xid);
				System.out.println(Color.cyan + "2PC::COMMIT sent to Flights successfully" + Color.reset);
				break;
			}
			catch (Exception e)
			{
				counter++;
				System.out.println(Color.red + "2PC::Could not send COMMIT to Flights. Attempt " + counter  + Color.reset);
				
				try 
				{
					Thread.sleep(recoveryCheckInterval);
					System.out.println(Color.yellow + "Attempting to rebind Flights" + Color.reset);
					
					//Manually re-lookup the RM from the registry
					Registry registry = LocateRegistry.getRegistry("lab2-4.cs.mcgill.ca", 1099);
					Middleware.m_flightsManager = (IResourceManager) registry.lookup("group33Flights");
					
				} 
				catch (Exception e1) 
				{
					System.out.println(Color.red + "Rebind failed" + Color.reset);
					e1.printStackTrace();
				}
			}
		}
		if (counter == 3) {
			System.out.println(Color.red + "2PC::Failed to send COMMIT to Flights. Data will be inaccessible (but not inconsistent)." +  Color.reset);
			abort(xid);	
			return false;
		}

		
		counter = 0;
		while(counter < 3) {
			try
			{
				System.out.println(Color.yellow + "2PC::Sending COMMIT to Cars Manager" + Color.reset);
				Middleware.m_carsManager.commit(xid);
				System.out.println(Color.cyan + "2PC::COMMIT sent to Cars successfully" + Color.reset);
				break;
			}
			catch (Exception e)
			{
				counter++;
				System.out.println(Color.red + "2PC::Could not send COMMIT to Cars. Attempt " + counter  + Color.reset);
				
				try 
				{
					Thread.sleep(recoveryCheckInterval);
					System.out.println(Color.yellow + "Attempting to rebind Cars" + Color.reset);
					
					//Manually re-lookup the RM from the registry
					Registry registry = LocateRegistry.getRegistry("lab1-10.cs.mcgill.ca", 1099);
					Middleware.m_carsManager = (IResourceManager) registry.lookup("group33Cars");
					
				} 
				catch (Exception e1) 
				{
					System.out.println(Color.red + "Rebind failed" + Color.reset);
					e1.printStackTrace();
				}
			}
		}
		if (counter == 3) {
			System.out.println(Color.red + "2PC::Failed to send COMMIT to Cars. Data will be inaccessible (but not inconsistent)." +  Color.reset);
			abort(xid);	
			return false;
		}
		
		
		//crash API
		if(crashMode==6)
		{
			System.out.println("Crash for mode: " + crashMode);
			System.exit(1);
		}
		
	
		counter = 0;
		while(counter < 3) {
			try
			{
				System.out.println(Color.yellow + "2PC::Sending COMMIT to Rooms Manager" + Color.reset);
				Middleware.m_roomsManager.commit(xid);
				System.out.println(Color.cyan + "2PC::COMMIT sent to Rooms successfully" + Color.reset);
				break;
			}
			catch (Exception e)
			{
				counter++;
				System.out.println(Color.red + "2PC::Could not send COMMIT to Rooms. Attempt " + counter  + Color.reset);
				
				try 
				{
					Thread.sleep(recoveryCheckInterval);
					System.out.println(Color.yellow + "Attempting to rebind Rooms" + Color.reset);
					
					//Manually re-lookup the RM from the registry
					Registry registry = LocateRegistry.getRegistry("open-4.cs.mcgill.ca", 1099);
					Middleware.m_roomsManager = (IResourceManager) registry.lookup("group33Rooms");
					
				} 
				catch (Exception e1) 
				{
					System.out.println(Color.red + "Rebind failed" + Color.reset);
					e1.printStackTrace();
				}
			}
		}
		if (counter == 3) {
			System.out.println(Color.red + "2PC::Failed to send COMMIT to Rooms. Data will be inaccessible (but not inconsistent)." +  Color.reset);
			abort(xid);	
			return false;
		}
		
		
		System.out.println(Color.yellow + "2PC::Sending COMMIT to Customers Manager" + Color.reset);
		
		// Perform the actual commit of customers == swapping to other disk file
		customersManager.fileManagerSwap();
		customersManager.updateStorage();
		
		System.out.println(Color.cyan + "2PC::COMMIT sent to Customers successfully" + Color.reset);
		
		log.removeTxLog(xid);
		log.flushLog();
		
		
		//crash API
		if(crashMode==7)
		{
			System.out.println("Crash for mode: " + crashMode);
			System.exit(1);
		}
		
		activeTransactions.remove((Integer) xid);
		transactionMap.remove(xid);
		toDeleteMap.remove(xid);
		lockManager.UnlockAll(xid);
		transactionTimers.remove(xid);
		
		System.out.println(Color.purp + "TM::Transaction: " + xid + " has commited" + Color.reset);
		resetRMTimers();
		return true;
		
	}	

	
	private RMItem readDataCopy(int xid, String key)
	{
		synchronized(m_dataCopy) {
			RMItem item = m_dataCopy.get(key);
			if (item != null) {
				return (RMItem)item.clone();
			}
			return null;
		}
	}


	private void writeDataCopy(int xid, String key, RMItem value)
	{
		synchronized(m_dataCopy) {
			m_dataCopy.put(key, value);
		}
	}

	private void removeDataCopy(int xid, String key)
	{
		synchronized(m_dataCopy) {
			m_dataCopy.remove(key);
		}
	}

	public void deleteDataCopy(int xid, String key){
		removeDataCopy(xid, key);
	}

	
	// Updated - Milestone 3
	public boolean addFlight(int xid, int flightNum, int flightSeats, int flightPrice) throws RemoteException, InvalidTransactionException, TransactionAbortedException
	{
		checkValid(xid);
		cancelTimer(xid);
		cancelRMTimers();
		
		boolean success = false;
		try
		{
			if(lockManager.Lock(xid, "flight-"+flightNum, TransactionLockObject.LockType.LOCK_WRITE)) 
			{
				success = Middleware.m_flightsManager.addFlight(xid, flightNum, flightSeats, flightPrice);				
			}
			else
			{
				//lockManager.Lock() returns false for invalid parameters
				Trace.info("Locking on flight-" + flightNum + " failed: invalid parameters.");
			}
			
			// TODO Perhaps if success is false it means the flight Manager crashed and we can handle this here ??
			
		}catch(DeadlockException e){
			abort(xid);
			abortedTransactions.add(xid);
			resetRMTimers();
			return false;
		}
		resetTimer(xid);
		resetRMTimers();
		
		return success;
	}

	
	
	public boolean addCars(int xid, String location, int count, int price) throws RemoteException, InvalidTransactionException, TransactionAbortedException
	{
		checkValid(xid);
		cancelTimer(xid);
		cancelRMTimers();
		
		boolean success = false;
		try
		{
			if(lockManager.Lock(xid, "car-"+location, TransactionLockObject.LockType.LOCK_WRITE))
			{
				success = Middleware.m_carsManager.addCars(xid, location, count, price);	
			}
			else
			{
				//lockManager.Lock() returns false for invalid parameters
				Trace.info("Locking on car-" + location + " failed: invalid parameters.");
			}

		}catch(DeadlockException e){
			abort(xid);
			abortedTransactions.add(xid);
			resetRMTimers();
			return false;
		}
		resetTimer(xid);
		resetRMTimers();
		return success;	
	}

	// signature: Adrian Koretski
	public boolean addRooms(int xid, String location, int count, int price) throws RemoteException, InvalidTransactionException, TransactionAbortedException
	{
		checkValid(xid);
		cancelTimer(xid);
		cancelRMTimers();
		
		boolean success = false;
		try{
			if(lockManager.Lock(xid, "room-"+location, TransactionLockObject.LockType.LOCK_WRITE))
			{
				success = Middleware.m_roomsManager.addRooms(xid, location, count, price);
			}
		}catch(DeadlockException e){
			abort(xid);
			abortedTransactions.add(xid);
			resetRMTimers();
			return false;
		}
		resetTimer(xid);
		resetRMTimers();
		
		return success;
	}
	
	

	public int newCustomer(int xid) throws RemoteException, InvalidTransactionException, TransactionAbortedException
	{
		checkValid(xid);
		cancelTimer(xid);
		cancelRMTimers();
		
		try{
			int cid = Integer.parseInt(String.valueOf(xid) + String.valueOf(Calendar.getInstance().get(Calendar.MILLISECOND)) + String.valueOf(Math.round(Math.random() * 100 + 1)));
			if(toDeleteMap.get(xid).contains("customer-"+cid)){
				toDeleteMap.get(xid).remove("customer-"+cid);
			}
			if(lockManager.Lock(xid, "customer-"+cid, TransactionLockObject.LockType.LOCK_WRITE)){
				Customer customer = new Customer(cid);
				writeDataCopy(xid, customer.getKey(), customer);
				transactionMap.get(xid).add(customer.getKey());
				Trace.info("Customer: " + cid + " has been written locally.");
				
				resetTimer(xid);
				resetRMTimers();
				return cid;
			}
		}catch(DeadlockException e){
			abort(xid);
			abortedTransactions.add(xid);
			
			resetRMTimers();
			return 0;
		}
		
		resetTimer(xid);
		resetRMTimers();
		
		return 0;
	}

	public boolean newCustomer(int xid, int customerId) throws RemoteException, InvalidTransactionException, TransactionAbortedException
	{
		checkValid(xid);
		cancelTimer(xid);
		cancelRMTimers();
		
		try{
			if(lockManager.Lock(xid, "customer-"+customerId, TransactionLockObject.LockType.LOCK_WRITE)){
				Customer customer = (Customer) readDataCopy(xid, "customer-"+ customerId);

				if(customer==null){
					Customer remoteCustomer = (Customer) customersManager.getItem(xid, "customer-"+customerId);
					if(remoteCustomer==null){
						Customer newCustomer = new Customer(customerId);
						writeDataCopy(xid, newCustomer.getKey(), newCustomer);
						transactionMap.get(xid).add(newCustomer.getKey());
						Trace.info("New Customer: "+customerId+" was sucessfully created i Local copy.");
						
						resetTimer(xid);
						resetRMTimers();
						return true;
					}
					else{
						Trace.info("Customer already exists in remote. Failed adding customer.");
						
						resetTimer(xid);
						resetRMTimers();
						return false;
					}
				}
				else{
					Trace.info("Customer already exists in Local Copy. Failed adding customer.");
					
					resetTimer(xid);
					resetRMTimers();
					return false;
				}
			}
		}catch(DeadlockException e){
			abort(xid);
			abortedTransactions.add(xid);
			resetRMTimers();
			return false;
		}
		resetTimer(xid);
		resetRMTimers();
		return true;
	}

	// Updated - Milestone 3
	public int queryFlight(int xid, int flightNum) throws RemoteException, InvalidTransactionException, TransactionAbortedException
	{
		checkValid(xid);
		cancelTimer(xid);
		cancelRMTimers();
		
		try{
			if(lockManager.Lock(xid, "flight-"+flightNum, TransactionLockObject.LockType.LOCK_READ))
			{
				resetTimer(xid);
				resetRMTimers();
				return Middleware.m_flightsManager.queryFlight(xid, flightNum);
			}
			else 
			{
				//lockManager.Lock returns false for invalid parameters
				Trace.info("Locking on flight-" + flightNum + " failed: invalid parameters.");
			}

		}catch(DeadlockException e){
			abort(xid);
			abortedTransactions.add(xid);
			resetRMTimers();
			return -1;
		}
		resetTimer(xid);
		resetRMTimers();
		return -1;
	}
	
	public int queryFlightPrice(int xid, int flightNum) throws RemoteException, InvalidTransactionException, TransactionAbortedException
	{
		checkValid(xid);
		cancelTimer(xid);
		cancelRMTimers();
		
		try{
			if(lockManager.Lock(xid, "flight-"+flightNum, TransactionLockObject.LockType.LOCK_READ)){
				resetTimer(xid);
				resetRMTimers();
				return Middleware.m_flightsManager.queryFlightPrice(xid, flightNum);
			}
			else{
				//lockManager.Lock returns false for invalid parameters
				Trace.info("Locking on flight-" + flightNum + " failed: invalid parameters.");
			}

		}catch(DeadlockException e){
			abort(xid);
			abortedTransactions.add(xid);
			resetRMTimers();
			return -1;
		}
		resetTimer(xid);
		resetRMTimers();
		return -1;
	}

	// signature: Adrian Koretski
	public int queryCars(int xid, String location) throws RemoteException, InvalidTransactionException, TransactionAbortedException
	{
		checkValid(xid);
		cancelTimer(xid);
		cancelRMTimers();
		
		try{
			if(lockManager.Lock(xid, "car-"+location, TransactionLockObject.LockType.LOCK_READ))
			{
				resetTimer(xid);
				resetRMTimers();
				return Middleware.m_carsManager.queryCars(xid, location);
			}
			else
			{
				//lockManager.Lock returns false for invalid parameters
				Trace.info("Locking on car-" + location + " failed: invalid parameters.");
			}
		}catch(DeadlockException e){
			abort(xid);
			abortedTransactions.add(xid);
			resetRMTimers();
			return -1;
		}
		resetTimer(xid);
		resetRMTimers();
		return -1;
	}
	

	// signature: Adrian Koretski
	public int queryCarsPrice(int xid, String location) throws RemoteException, InvalidTransactionException, TransactionAbortedException
	{
		checkValid(xid);
		cancelTimer(xid);
		cancelRMTimers();
	
		try{
			if(lockManager.Lock(xid, "car-"+location, TransactionLockObject.LockType.LOCK_READ))
			{
				resetTimer(xid);
				resetRMTimers();
				return Middleware.m_carsManager.queryCarsPrice(xid, location);
			}
			else
			{
				Trace.info("Locking on car-" + location + " failed: invalid parameters.");
			}
		}catch(DeadlockException e){
			abort(xid);
			abortedTransactions.add(xid);
			resetRMTimers();
			return -1;
		}
		resetTimer(xid);
		resetRMTimers();
		return -1;
	}

	// signature: Adrian Koretski
	public int queryRooms(int xid, String location) throws RemoteException, InvalidTransactionException, TransactionAbortedException
	{
		checkValid(xid);
		cancelTimer(xid);
		cancelRMTimers();
		try
		{
			if(lockManager.Lock(xid, "room-"+location, TransactionLockObject.LockType.LOCK_READ))
			{
				resetTimer(xid);
				resetRMTimers();
				return Middleware.m_roomsManager.queryRooms(xid, location);
			}
			else
			{
				Trace.info("Locking on room-" + location + " failed: invalid parameters.");
			}
		}catch(DeadlockException e){
			abort(xid);
			abortedTransactions.add(xid);
			resetRMTimers();
			return -1;
		}
		resetTimer(xid);
		resetRMTimers();
		return -1;
	}

	public int queryRoomsPrice(int xid, String location) throws RemoteException, InvalidTransactionException, TransactionAbortedException
	{
		checkValid(xid);
		cancelTimer(xid);
		cancelRMTimers();
		try{
			if(lockManager.Lock(xid, "room-"+location, TransactionLockObject.LockType.LOCK_READ))
			{
				resetTimer(xid);
				resetRMTimers();
				return Middleware.m_roomsManager.queryRoomsPrice(xid, location);
			}
			else
			{
				Trace.info("Locking on car-" + location + " failed: invalid parameters.");
			}
		}catch(DeadlockException e){
			abort(xid);
			abortedTransactions.add(xid);
			resetRMTimers();
			return -1;
		}
		resetTimer(xid);
		resetRMTimers();
		return -1;
	}
	
	public String queryCustomerInfo(int xid, int cid) throws RemoteException, InvalidTransactionException, TransactionAbortedException
	{
		checkValid(xid);
		cancelTimer(xid);
		cancelRMTimers();
		String str = "customer-" + cid;
		try {
			if (lockManager.Lock(xid, str, TransactionLockObject.LockType.LOCK_READ))
			{
				if (toDeleteMap.get(xid).contains(str)) {
					Trace.info("Customer " + cid +" was deleted and cannot be accessed.");
					resetTimer(xid);
					resetRMTimers();
					return null;
				}
				else 
				{
					Customer customer = (Customer) readDataCopy(xid, "customer-"+ cid);
					
					if(customer == null)
					{
						Customer r_customer = (Customer) customersManager.getItem(xid, "customer-"+cid);
						if(r_customer == null)
						{
							Trace.warn("Customer does not exist in the local copy or in the remote server. Query Failed");
							resetTimer(xid);
							resetRMTimers();
							return null;
						}
						else
						{
							writeDataCopy(xid, r_customer.getKey(), r_customer);
							transactionMap.get(xid).add(r_customer.getKey());
							resetTimer(xid);
							resetRMTimers();
							return r_customer.getBill();
						}
					}
					else 
					{
						writeDataCopy(xid, customer.getKey(), customer);
						transactionMap.get(xid).add(customer.getKey());
						resetTimer(xid);
						resetRMTimers();
						return customer.getBill();
					}
				}
			}
		}
		catch (DeadlockException e) {
			abort(xid);
			abortedTransactions.add(xid);
			resetRMTimers();
			return null;
		}
				
		resetTimer(xid);
		resetRMTimers();
		return null;
	}
	
	//Milestone 3
	//updated
	public boolean reserveFlight(int xid, int customerID, int flightNum) throws RemoteException, InvalidTransactionException, TransactionAbortedException{
		checkValid(xid);
		cancelTimer(xid);
		cancelRMTimers();
		try{
			if(lockManager.Lock(xid, "customer-"+customerID, TransactionLockObject.LockType.LOCK_WRITE) && lockManager.Lock(xid, "flight-"+flightNum, TransactionLockObject.LockType.LOCK_WRITE)){
				
				//if the customer was deleted, should not be able to access customer or reserve the flight
				if(toDeleteMap.get(xid).contains("customer-"+customerID)){
					Trace.info("This customer: "+ "customer-"+customerID +" was deleted and cannot be accessed.");
					resetTimer(xid);
					resetRMTimers();
					return false;
				}

				//check location of customer, if it doesnt exist return false and trace 
				Customer curCustomer = (Customer) readDataCopy(xid, "customer-"+customerID);
				if(curCustomer == null){
					Customer remoteCustomer = (Customer) customersManager.getItem(xid, "customer-"+customerID);
					if(remoteCustomer == null){
						Trace.warn("customer does not exist in the local copy or in the remote server. Failed reservation.");
						resetTimer(xid);
						resetRMTimers();
						return false;
					}
					else{
						writeDataCopy(xid, remoteCustomer.getKey(), remoteCustomer);
						transactionMap.get(xid).add(remoteCustomer.getKey());
					}
				}

				//read reserveFlight in  resource manager
				if(!Middleware.m_flightsManager.reserveFlight(xid, customerID, flightNum)){
					Trace.info("Flight: " + "flight-" + flightNum + " does not exist or it is full, thus we cannot reserve it.");
					resetTimer(xid);
					resetRMTimers();
					return false;
				}

				Customer finalCustomer = (Customer) readDataCopy(xid, "customer-"+customerID);

				Flight finalItem = new Flight(flightNum, Middleware.m_flightsManager.queryFlight(xid, flightNum), Middleware.m_flightsManager.queryFlightPrice(xid, flightNum));

				finalCustomer.reserve(finalItem.getKey(), String.valueOf(flightNum), finalItem.getPrice());
				writeDataCopy(xid, finalCustomer.getKey(), finalCustomer);

				Trace.info("Reservation succeeded:" +finalItem.getKey()+finalCustomer.getKey());
				resetTimer(xid);
				resetRMTimers();
				return true;
			}
		}catch(DeadlockException e){
			abort(xid);
			abortedTransactions.add(xid);
			resetRMTimers();
			return false;
		}
		resetTimer(xid);
		resetRMTimers();
		return false;

	}

	//milestone 3
	//update in progress
	//check reserveflight comments for info
	public boolean reserveCar(int xid, int customerID, String location) throws RemoteException, InvalidTransactionException, TransactionAbortedException
	{
		checkValid(xid);
		cancelTimer(xid);
		cancelRMTimers();
		try{
			if(lockManager.Lock(xid, "customer-"+customerID, TransactionLockObject.LockType.LOCK_WRITE) && lockManager.Lock(xid, "car-"+location, TransactionLockObject.LockType.LOCK_WRITE)){
				if(toDeleteMap.get(xid).contains("car-"+location) || toDeleteMap.get(xid).contains("customer-"+customerID)){
					Trace.info("This car or customer was deleted and cannot be accessed.");
					resetTimer(xid);
					resetRMTimers();
					return false;
				}

				Customer curCustomer = (Customer) readDataCopy(xid, "customer-"+customerID);
				if(curCustomer == null){
					Customer remoteCustomer = (Customer) customersManager.getItem(xid, "customer-"+customerID);
					if(remoteCustomer == null){
						Trace.warn("customer does not exist in the local copy or in the remote server. Failed reservation.");
						resetTimer(xid);
						resetRMTimers();
						return false;
					}
					else{
						writeDataCopy(xid, remoteCustomer.getKey(), remoteCustomer);
						transactionMap.get(xid).add(remoteCustomer.getKey());
					}
				}

				if(!Middleware.m_carsManager.reserveCar(xid, customerID, location)){
					Trace.info("Cars: " + "car-" + location + " does not exist or it is full, thus we cannot reserve it.");
					resetTimer(xid);
					resetRMTimers();
					return false;
				}

				Customer finalCustomer = (Customer) readDataCopy(xid, "customer-"+customerID);

				Car finalItem = new Car(location, Middleware.m_carsManager.queryCars(xid, location), Middleware.m_carsManager.queryCarsPrice(xid, location));

				finalCustomer.reserve("car-"+location, location, finalItem.getPrice());
				writeDataCopy(xid, finalCustomer.getKey(), finalCustomer);

				Trace.info("Reservation succeeded:" +finalItem.getKey()+finalCustomer.getKey());
				resetTimer(xid);
				resetRMTimers();
				return true;

			}
		}catch(DeadlockException e){
			abort(xid);
			abortedTransactions.add(xid);
			resetRMTimers();
			return false;
		}
		resetTimer(xid);
		resetRMTimers();
		return false;
	}

	public boolean reserveRoom(int xid, int customerID, String location) throws RemoteException, InvalidTransactionException, TransactionAbortedException
	{
		checkValid(xid);
		cancelTimer(xid);
		cancelRMTimers();
		try{
			if(lockManager.Lock(xid, "customer-"+customerID, TransactionLockObject.LockType.LOCK_WRITE) && lockManager.Lock(xid, "room-"+location, TransactionLockObject.LockType.LOCK_WRITE)){
				if(toDeleteMap.get(xid).contains("room-"+location) || toDeleteMap.get(xid).contains("customer-"+customerID)){
					Trace.info("This room or customer was deleted and cannot be accessed.");
					resetTimer(xid);
					resetRMTimers();
					return false;
				}

				Customer curCustomer = (Customer) readDataCopy(xid, "customer-"+customerID);
				if(curCustomer == null){
					Customer remoteCustomer = (Customer) customersManager.getItem(xid, "customer-"+customerID);
					if(remoteCustomer == null){
						Trace.warn("customer does not exist in the local copy or in the remote server. Failed reservation.");
						resetTimer(xid);
						resetRMTimers();
						return false;
					}
					else{
						// We put the customer in the local copy, to be used right after
						writeDataCopy(xid, remoteCustomer.getKey(), remoteCustomer);
						transactionMap.get(xid).add(remoteCustomer.getKey());
					}
				}

				if(!Middleware.m_roomsManager.reserveRoom(xid, customerID, location))
				{
					Trace.warn("Rooms: "+ "room-"+location+" does not exist or it is full. Reservation failed.");
					resetTimer(xid);
					resetRMTimers();
					return false;
				}

				// Read the 
				Customer finalCustomer = (Customer) readDataCopy(xid, "customer-"+customerID);

				Room finalItem = new Room(location, Middleware.m_roomsManager.queryRooms(xid, location), Middleware.m_roomsManager.queryRoomsPrice(xid, location));

				finalCustomer.reserve("room-"+location, location, finalItem.getPrice());
				writeDataCopy(xid, finalCustomer.getKey(), finalCustomer);

				Trace.info("Reservation succeeded:" +finalItem.getKey()+finalCustomer.getKey());
				
				resetTimer(xid);
				resetRMTimers();
				return true;

			}

		}catch(DeadlockException e){
			abort(xid);
			abortedTransactions.add(xid);
			resetRMTimers();
			return false;
		}
		resetTimer(xid);
		resetRMTimers();
		return false;
	}

	public boolean deleteFlight(int xid, int flightNum) throws RemoteException, InvalidTransactionException, TransactionAbortedException
	{
		checkValid(xid);
		cancelTimer(xid);
		cancelRMTimers();
		try{
			if(lockManager.Lock(xid, "flight-"+flightNum, TransactionLockObject.LockType.LOCK_WRITE))
			{
				resetTimer(xid);
				resetRMTimers();
				return Middleware.m_flightsManager.deleteFlight(xid, flightNum);
			}
			else {
				// Invalid Locking parameters.
			}


		}catch(DeadlockException e){
			abort(xid);
			abortedTransactions.add(xid);
			resetRMTimers();
			return false;
		}
		resetTimer(xid);
		resetRMTimers();
		return false;
	}

	public boolean deleteCars(int xid, String location) throws RemoteException, InvalidTransactionException, TransactionAbortedException
	{
		checkValid(xid);
		cancelTimer(xid);
		cancelRMTimers();
		boolean inRM = false;
		try{
			if(lockManager.Lock(xid, "car-"+location, TransactionLockObject.LockType.LOCK_WRITE))
			{
				resetTimer(xid);
				resetRMTimers();
				return Middleware.m_carsManager.deleteCars(xid, location);
			}
			else {
				// Invalid Locking parameters.
			}
		}catch(DeadlockException e){
			abort(xid);
			abortedTransactions.add(xid);
			resetRMTimers();
			return false;
		}
		resetTimer(xid);
		resetRMTimers();
		return false;
	}

	public boolean deleteRooms(int xid, String location) throws RemoteException, InvalidTransactionException, TransactionAbortedException
	{
		checkValid(xid);
		cancelTimer(xid);
		cancelRMTimers();
		boolean inRM = false;
		
		try{
			if(lockManager.Lock(xid, "room-"+location, TransactionLockObject.LockType.LOCK_WRITE))
			{
				resetTimer(xid);
				resetRMTimers();
				return Middleware.m_roomsManager.deleteRooms(xid, location);
			}
			else {
				// Invalid Locking parameters.
			}

		}catch(DeadlockException e){
			abort(xid);
			abortedTransactions.add(xid);
			resetRMTimers();
			return false;
		}
		resetTimer(xid);
		resetRMTimers();
		return false;
	}


	//Milestone 3 update in progress
	public boolean deleteCustomer(int xid, int customerID) throws RemoteException, InvalidTransactionException, TransactionAbortedException
	{
		checkValid(xid);
		cancelTimer(xid);
		cancelRMTimers();
		
		try
		{
			if(lockManager.Lock(xid, "customer-"+customerID, TransactionLockObject.LockType.LOCK_WRITE))
			{
				Customer curObj = (Customer) readDataCopy(xid, "customer-"+customerID);
				if(curObj == null)
				{
					Customer remoteObj = (Customer) customersManager.getItem(xid, "customer-"+customerID);
					if(remoteObj == null){
						Trace.warn("the customer does not exist. Deletion failed.");
						resetTimer(xid);
						resetRMTimers();
						return false;
					}
					else{
						Trace.info("Customer was found in RM");
						writeDataCopy(xid, remoteObj.getKey(), remoteObj);
						transactionMap.get(xid).add(remoteObj.getKey());
					}
				}else Trace.info("found in local copy customer.");

				Customer finalCustomer = (Customer) readDataCopy(xid, "customer-"+customerID);
				RMHashMap reservations = finalCustomer.getReservations();
				for (String reservedKey : reservations.keySet())
				{
					if(lockManager.Lock(xid, reservedKey, TransactionLockObject.LockType.LOCK_WRITE)){
						ReservedItem reserveditem = finalCustomer.getReservedItem(reservedKey);
						Trace.info("RM::deleteCustomer(" + xid + ", " + customerID + ") has reserved " + reserveditem.getKey() + " " +  reserveditem.getCount() +  " times");
						
						if(reservedKey.startsWith("flight")){
							Middleware.m_flightsManager.changeObject(xid, reservedKey, reserveditem.getCount());
						}
						else if(reservedKey.startsWith("car")){
							Middleware.m_carsManager.changeObject(xid, reservedKey, reserveditem.getCount());
						}
						else if(reservedKey.startsWith("room")){
							Middleware.m_roomsManager.changeObject(xid, reservedKey, reserveditem.getCount());
						}

					}
						
				}
				Trace.info("All reserved items from the customer have been reset.");


				Trace.info("Cust added to delete map");
				toDeleteMap.get(xid).add(finalCustomer.getKey());					
				removeDataCopy(xid, finalCustomer.getKey());
				transactionMap.get(xid).remove(finalCustomer.getKey());
				
				resetTimer(xid);
				resetRMTimers();
				return true;
			}
		}catch(DeadlockException e){
			abort(xid);
			abortedTransactions.add(xid);
			resetRMTimers();
			return false;
		}
		resetTimer(xid);
		resetRMTimers();
		return false;
	}
	
	//TODO Query everything first so it is all or nothing
	
	public boolean bundle(int xid, int customerID, Vector<String> flightNumbers, String location, boolean car, boolean room) throws RemoteException, InvalidTransactionException, TransactionAbortedException 
	{
		checkValid(xid);
		cancelTimer(xid);
		cancelRMTimers();
		
		String str = "customer-" + customerID;
		
		// Attempt to query flights, cars and rooms before locking on customer and return false if one doesn't exist
		for (String num : flightNumbers) 
		{
			try {
				if(lockManager.Lock(xid, "flight-"+num, TransactionLockObject.LockType.LOCK_READ)){
					
					if(queryFlightBundle(xid, Integer.parseInt(num))==-1){
						Trace.info("flight does not exist abort bundle.");
						resetTimer(xid);
						resetRMTimers();
						return false;
					}
				}
				else {
					System.out.println("Could not get read lock on flights");
				}
			} catch (Exception e) {
				e.printStackTrace();			
			}
		}
		
		try {
			if(car && lockManager.Lock(xid, "car-"+location, TransactionLockObject.LockType.LOCK_READ))
			{
				if(queryCarsBundle(xid, location) == -1){
					Trace.info("car at this location does not exist. abort bundle.");
					resetTimer(xid);
					resetRMTimers();
					return false;
				}
				System.out.println(queryCars(xid, "car-" + location));
			}
			else {
				System.out.println("could not get read lock on cars");
			}
		} catch (DeadlockException e1) {
			e1.printStackTrace();
		}
		
		try {
			if(room && lockManager.Lock(xid,  "room-"+location, TransactionLockObject.LockType.LOCK_READ))
			{
				if(queryRoomsBundle(xid, location) == -1){
					Trace.info("rooms at this location does not exist. abort bundle.");
					resetTimer(xid);
					resetRMTimers();
					return false;
				}
				System.out.println(queryCars(xid, "room-" + location));
			}
		} catch (DeadlockException e1) {
			e1.printStackTrace();
		}
		
		try {
			if (lockManager.Lock(xid, str, TransactionLockObject.LockType.LOCK_WRITE))
			{
				Customer curObj = (Customer) readDataCopy(xid, "customer-"+customerID);
				if(curObj == null)
				{
					Customer remoteObj = (Customer) customersManager.getItem(xid, "customer-"+customerID);
					if(remoteObj == null)
					{
						Trace.warn("Customer " + customerID + " does not exist. Bundle failed.");
						resetTimer(xid);
						resetRMTimers();
						return false;
					}
					else{
						writeDataCopy(xid, remoteObj.getKey(), remoteObj);
						transactionMap.get(xid).add(remoteObj.getKey());
					}
				}
				
				
				
				for (String num : flightNumbers) 
				{
					reserveFlightBundle(xid, customerID, Integer.parseInt(num));
				}
				if (car) 
				{
					reserveCarBundle(xid, customerID, location);
				}
				if (room) 
				{
					reserveRoomBundle(xid, customerID, location);

				}
			}
		}
		catch (DeadlockException e) {
			abort(xid);
			abortedTransactions.add(xid);
			resetRMTimers();
			return false;
		}
		
		resetTimer(xid);
		resetRMTimers();
		return true;
	}
	
	// utility function for bundle to call
	// No use of timers else we would get some lost Timer threads running around leading to transaction timeouts after commiting (not fun)
	private int queryCarsBundle(int xid, String location) throws RemoteException, InvalidTransactionException, TransactionAbortedException
	{
		checkValid(xid);
		
		try{
			if(lockManager.Lock(xid, "car-"+location, TransactionLockObject.LockType.LOCK_READ))
			{
				return Middleware.m_carsManager.queryCars(xid, location);
			}
			else
			{
				//lockManager.Lock returns false for invalid parameters
				Trace.info("Locking on car-" + location + " failed: invalid parameters.");
			}
		}catch(DeadlockException e){
			abort(xid);
			abortedTransactions.add(xid);
			return -1;
		}
		return -1;
	}
	
	
	// utility function for bundle to call
	// No use of timers else we would get some lost Timer threads running around leading to transaction timeouts after commiting (not fun)
	private int queryFlightBundle(int xid, int flightNum) throws RemoteException, InvalidTransactionException, TransactionAbortedException
	{
		checkValid(xid);
		try{
			if(lockManager.Lock(xid, "flight-"+flightNum, TransactionLockObject.LockType.LOCK_READ))
			{
				return Middleware.m_flightsManager.queryFlight(xid, flightNum);
			}
			else 
			{
				//lockManager.Lock returns false for invalid parameters
				Trace.info("Locking on flight-" + flightNum + " failed: invalid parameters.");
			}
		}catch(DeadlockException e){
			abort(xid);
			abortedTransactions.add(xid);
			return -1;
		}
		return -1;
	}
	
	private boolean reserveFlightBundle(int xid, int customerID, int flightNum) throws RemoteException, InvalidTransactionException, TransactionAbortedException{
		checkValid(xid);

		try{
			if(lockManager.Lock(xid, "customer-"+customerID, TransactionLockObject.LockType.LOCK_WRITE) && lockManager.Lock(xid, "flight-"+flightNum, TransactionLockObject.LockType.LOCK_WRITE)){
				
				//if the customer was deleted, should not be able to access customer or reserve the flight
				if(toDeleteMap.get(xid).contains("customer-"+customerID)){
					Trace.info("This customer: "+ "customer-"+customerID +" was deleted and cannot be accessed.");

					return false;
				}

				//check location of customer, if it doesnt exist return false and trace 
				Customer curCustomer = (Customer) readDataCopy(xid, "customer-"+customerID);
				if(curCustomer == null)
				{
					Customer remoteCustomer = (Customer) customersManager.getItem(xid, "customer-"+customerID);
					if(remoteCustomer == null){
						Trace.warn("customer does not exist in the local copy or in the remote server. Failed reservation.");

						return false;
					}
					else{
						writeDataCopy(xid, remoteCustomer.getKey(), remoteCustomer);
						transactionMap.get(xid).add(remoteCustomer.getKey());
					}
				}

				//read reserveFlight in  resource manager
				if(!Middleware.m_flightsManager.reserveFlight(xid, customerID, flightNum)){
					Trace.info("Flight: " + "flight-" + flightNum + " does not exist or it is full, thus we cannot reserve it.");

					return false;
				}

				Customer finalCustomer = (Customer) readDataCopy(xid, "customer-"+customerID);

				Flight finalItem = new Flight(flightNum, Middleware.m_flightsManager.queryFlight(xid, flightNum), Middleware.m_flightsManager.queryFlightPrice(xid, flightNum));

				finalCustomer.reserve(finalItem.getKey(), String.valueOf(flightNum), finalItem.getPrice());
				writeDataCopy(xid, finalCustomer.getKey(), finalCustomer);

				Trace.info("Reservation succeeded:" +finalItem.getKey()+finalCustomer.getKey());

				return true;
			}
		}catch(DeadlockException e){
			abort(xid);
			abortedTransactions.add(xid);

			return false;
		}
		
		return false;

	}
	
	// utility function for bundle to call
	// No use of timers else we would get some lost Timer threads running around leading to transaction timeouts after commiting (not fun)
	private int queryRoomsBundle(int xid, String location) throws RemoteException, InvalidTransactionException, TransactionAbortedException
	{
		checkValid(xid);
		
		try {
			if(lockManager.Lock(xid, "room-"+location, TransactionLockObject.LockType.LOCK_READ))
			{
				return Middleware.m_roomsManager.queryRooms(xid, location);
			}
			else
			{
				Trace.info("Locking on room-" + location + " failed: invalid parameters.");
			}
		}catch(DeadlockException e){
			abort(xid);
			abortedTransactions.add(xid);
			return -1;
		}
		return -1;
	}

	
	private boolean reserveRoomBundle(int xid, int customerID, String location) throws RemoteException, InvalidTransactionException, TransactionAbortedException
	{
		checkValid(xid);

		try{
			if(lockManager.Lock(xid, "customer-"+customerID, TransactionLockObject.LockType.LOCK_WRITE) && lockManager.Lock(xid, "room-"+location, TransactionLockObject.LockType.LOCK_WRITE)){
				if(toDeleteMap.get(xid).contains("room-"+location) || toDeleteMap.get(xid).contains("customer-"+customerID)){
					Trace.info("This room or customer was deleted and cannot be accessed.");

					return false;
				}

				Customer curCustomer = (Customer) readDataCopy(xid, "customer-"+customerID);
				if(curCustomer == null){
					Customer remoteCustomer = (Customer) customersManager.getItem(xid, "customer-"+customerID);
					if(remoteCustomer == null){
						Trace.warn("customer does not exist in the local copy or in the remote server. Failed reservation.");

						return false;
					}
					else{
						// We put the customer in the local copy, to be used right after
						writeDataCopy(xid, remoteCustomer.getKey(), remoteCustomer);
						transactionMap.get(xid).add(remoteCustomer.getKey());
					}
				}

				if(!Middleware.m_roomsManager.reserveRoom(xid, customerID, location))
				{
					Trace.warn("Rooms: "+ "room-"+location+" does not exist or it is full. Reservation failed.");

					return false;
				}

				// Read the 
				Customer finalCustomer = (Customer) readDataCopy(xid, "customer-"+customerID);

				Room finalItem = new Room(location, Middleware.m_roomsManager.queryRooms(xid, location), Middleware.m_roomsManager.queryRoomsPrice(xid, location));

				finalCustomer.reserve("room-"+location, location, finalItem.getPrice());
				writeDataCopy(xid, finalCustomer.getKey(), finalCustomer);

				Trace.info("Reservation succeeded:" +finalItem.getKey()+finalCustomer.getKey());
				

				return true;

			}

		}catch(DeadlockException e){
			abort(xid);
			abortedTransactions.add(xid);

			return false;
		}

		return false;
	}
	
	private boolean reserveCarBundle(int xid, int customerID, String location) throws RemoteException, InvalidTransactionException, TransactionAbortedException
	{
		checkValid(xid);

		try{
			if(lockManager.Lock(xid, "customer-"+customerID, TransactionLockObject.LockType.LOCK_WRITE) && lockManager.Lock(xid, "car-"+location, TransactionLockObject.LockType.LOCK_WRITE)){
				if(toDeleteMap.get(xid).contains("car-"+location) || toDeleteMap.get(xid).contains("customer-"+customerID)){
					Trace.info("This car or customer was deleted and cannot be accessed.");

					return false;
				}

				Customer curCustomer = (Customer) readDataCopy(xid, "customer-"+customerID);
				if(curCustomer == null){
					Customer remoteCustomer = (Customer) customersManager.getItem(xid, "customer-"+customerID);
					if(remoteCustomer == null){
						Trace.warn("customer does not exist in the local copy or in the remote server. Failed reservation.");

						return false;
					}
					else{
						writeDataCopy(xid, remoteCustomer.getKey(), remoteCustomer);
						transactionMap.get(xid).add(remoteCustomer.getKey());
					}
				}

				if(!Middleware.m_carsManager.reserveCar(xid, customerID, location)){
					Trace.info("Cars: " + "car-" + location + " does not exist or it is full, thus we cannot reserve it.");

					return false;
				}

				Customer finalCustomer = (Customer) readDataCopy(xid, "customer-"+customerID);

				Car finalItem = new Car(location, Middleware.m_carsManager.queryCars(xid, location), Middleware.m_carsManager.queryCarsPrice(xid, location));

				finalCustomer.reserve("car-"+location, location, finalItem.getPrice());
				writeDataCopy(xid, finalCustomer.getKey(), finalCustomer);

				Trace.info("Reservation succeeded:" +finalItem.getKey()+finalCustomer.getKey());

				return true;

			}
		}catch(DeadlockException e){
			abort(xid);
			abortedTransactions.add(xid);

			return false;
		}

		return false;
	}
	
	
	
	private void cancelTimer(int xid) {
		transactionTimers.get(xid).cancel();		
	}
	
	private void resetTimer(int xid) 
	{
		transactionTimers.get(xid).cancel();
		transactionTimers.remove(xid);
		Timer timer = new Timer();
		timer.schedule(new TimerTask() {
			@Override
			public void run() {
				Trace.info("Transaction " + xid + " timed out");
				try {
					abort(xid);
					abortedTransactions.add(xid);
				} catch (InvalidTransactionException | RemoteException e) {
					e.printStackTrace();
				}
			}
		}, TTL);
		transactionTimers.put(xid, timer);
	}
	
	private void checkValid(int xid) throws InvalidTransactionException, TransactionAbortedException {
		if (!activeTransactions.contains((Integer) xid)) {
			// If the txid is not in the active transactions list then either it does not exist 
			// or it was silently aborted since last time it was called.
			if (abortedTransactions.contains((Integer) xid)) {
				// remove it 
				abortedTransactions.remove((Integer) xid);
				throw new TransactionAbortedException(xid, null);
			}
			throw new InvalidTransactionException(xid, null);
		}
	}



	

	
	


}