package Server.Middleware;

import Server.Interface.*;

import java.rmi.RemoteException;

import Server.Common.*;
import Server.LockManager.*;
import Server.Middleware.TransactionManager;

import java.util.*;

import Server.Interface.InvalidTransactionException;
import Server.Interface.TransactionAbortedException;

import java.rmi.RemoteException;
import java.io.*;

public class TransactionManager {

	protected int xidCounter = 0;
	final int TTL = 40000;

	private LockManager lockManager = new LockManager();

	private ArrayList<Integer> transactions = new ArrayList<Integer>();
	private ArrayList<Integer> abortedTransactions = new ArrayList<Integer>();

	protected RMHashMap m_dataCopy = new RMHashMap();

	private Map<Integer, ArrayList<String>> transactionMap = new HashMap<>();
	private Map<Integer, ArrayList<String>> toDeleteMap = new HashMap<>();
	
	private Map<Integer, Timer> transactionTimers = new HashMap<Integer, Timer>();
	
	private IResourceManager customersManager = null;
	

	public TransactionManager(IResourceManager custs)
	{
		xidCounter = 0;
		this.customersManager = custs;

	}
	

	// Updated - Milestone 3
	/**
	 * Transactions now exist at each resource manager. They will thus have an associated transaction map and to delete map at each RM, even if
	 * they don't actually access any items from these RMs.
	 * 
	 */
	public int start() throws RemoteException {
		int xid = ++xidCounter;
		transactions.add(xid);
		
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
		
		// NEW : We now call start on each resource manager
		Middleware.m_flightsManager.start(xid);
		Middleware.m_carsManager.start(xid);
		Middleware.m_roomsManager.start(xid);
		// See not in  abort
		//customersManager.start(xid);
		
		Trace.info("TM::start() Transaction " + xid + " started");
		return xid;
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
		if (!transactions.contains(xid)) 
		{
			throw new InvalidTransactionException(xid, null);
		}
		else
		{
			// NEW
			
			transactionTimers.get(xid).cancel();
			transactionTimers.remove(xid);
			
			Middleware.m_flightsManager.abort(xid);
			Middleware.m_carsManager.abort(xid);
			Middleware.m_roomsManager.abort(xid);
			
			//TODO 
			//abort customers
			// Remove all local copy objects associated with this transaction
			for (String key : transactionMap.get(xid)) 
			{
				removeDataCopy(xid, key);
			}
			
			abortedTransactions.add(xid);
			transactions.remove((Integer) xid);
			transactionMap.remove(xid);
			toDeleteMap.remove(xid);
			lockManager.UnlockAll(xid);
			
			System.out.println("From TM: " + xid + " ABORTED");
		}
	}



	public boolean shutdown() throws RemoteException{
		return false;
	}

	// Updated - Milestone 3
	// TODO Need support for Middleware -- see abort
	public boolean commit(int xid) throws RemoteException, InvalidTransactionException, TransactionAbortedException
	{
		checkValid(xid);
		cancelTimer(xid);

		if (!transactions.contains(xid)) throw new InvalidTransactionException(xid, null);
		
		Middleware.m_flightsManager.commit(xid);
		Middleware.m_carsManager.commit(xid);
		Middleware.m_roomsManager.commit(xid);
		
		// See notes in abort
		//commit customers:
		// Iterate through the customers the transaction has created or modified and write them to storage.
		for (String key : transactionMap.get(xid)) 
		{
			RMItem toCommit = readDataCopy(xid, key);
			if (toCommit == null) {
				break;
			}
			else {
				System.out.println("ready to write customer" + toCommit);
				customersManager.putItem(xid, key, toCommit);
			}
			Trace.info("@commit time: removing " + key + " from local copy");
			removeDataCopy(xid, key);
		}
		
		// Iterate through the items the transaction marked for deletion and remove them from storage.
		/**
		 * Had to stop removing the items as we were iterating over the ArrayList because we would get the exception linked below.
		 * Instead we simply remove the whole Arraylist once we're done deleting each item inside of it.
		 * Could this cause errors with other transactions trying to do stuff concurrently?
		 * see https://stackoverflow.com/questions/223918/iterating-through-a-collection-avoiding-concurrentmodificationexception-when-mo
		 */
		for (String key : toDeleteMap.get(xid)) {
			if (key == null) {
				break;
			}
			else {
				System.out.println(key + " found in delete map, should be removed.");
				customersManager.deleteData(xid, key);
			}
		}

		
		// NOTE: Need the cast to Integer to remove the actual Integer xid, else since xid is an int it will consider it an index
		// This will lead to ArrayIndexOutOfBoundsException
		transactions.remove((Integer) xid);
		transactionMap.remove(xid);
		toDeleteMap.remove(xid);
		lockManager.UnlockAll(xid);
		transactionTimers.remove(xid);
		
		System.out.println("Transaction: " + xid + " has commited");
		return true;
	}

	
	
	
	
	protected RMItem readDataCopy(int xid, String key)
	{
		synchronized(m_dataCopy) {
			RMItem item = m_dataCopy.get(key);
			if (item != null) {
				return (RMItem)item.clone();
			}
			return null;
		}
	}


	protected void writeDataCopy(int xid, String key, RMItem value)
	{
		synchronized(m_dataCopy) {
			m_dataCopy.put(key, value);
		}
	}

	protected void removeDataCopy(int xid, String key)
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
			return false;
		}
		resetTimer(xid);
		return success;
	}

	public boolean addCars(int xid, String location, int count, int price) throws RemoteException, InvalidTransactionException, TransactionAbortedException
	{
		checkValid(xid);
		cancelTimer(xid);
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
			return false;
		}
		resetTimer(xid);
		return success;	
	}

	// signature: Adrian Koretski
	public boolean addRooms(int xid, String location, int count, int price) throws RemoteException, InvalidTransactionException, TransactionAbortedException
	{
		checkValid(xid);
		cancelTimer(xid);
		boolean success = false;
		try{
			if(lockManager.Lock(xid, "room-"+location, TransactionLockObject.LockType.LOCK_WRITE))
			{
				success = Middleware.m_carsManager.addCars(xid, location, count, price);
			}
		}catch(DeadlockException e){
			abort(xid);
			abortedTransactions.add(xid);
			return false;
		}
		resetTimer(xid);
		return success;
	}
	
	

	public int newCustomer(int xid) throws RemoteException, InvalidTransactionException, TransactionAbortedException
	{
		checkValid(xid);
		cancelTimer(xid);
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
				return cid;
			}
		}catch(DeadlockException e){
			abort(xid);
			abortedTransactions.add(xid);
			return 0;
		}
		resetTimer(xid);
		return 0;
	}

	public boolean newCustomer(int xid, int customerId) throws RemoteException, InvalidTransactionException, TransactionAbortedException
	{
		checkValid(xid);
		cancelTimer(xid);
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
						return true;
					}
					else{
						Trace.info("Customer already exists in remote. Failed adding customer.");
						resetTimer(xid);
						return false;
					}
				}
				else{
					Trace.info("Customer already exists in Local Copy. Failed adding customer.");
					resetTimer(xid);
					return false;
				}
			}
		}catch(DeadlockException e){
			abort(xid);
			abortedTransactions.add(xid);
			return false;
		}
		resetTimer(xid);
		return true;
	}

	// Updated - Milestone 3
	public int queryFlight(int xid, int flightNum) throws RemoteException, InvalidTransactionException, TransactionAbortedException
	{
		checkValid(xid);
		cancelTimer(xid);
		try{
			if(lockManager.Lock(xid, "flight-"+flightNum, TransactionLockObject.LockType.LOCK_READ))
			{
				resetTimer(xid);
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
		resetTimer(xid);
		return -1;
	}
	
	public int queryFlightPrice(int xid, int flightNum) throws RemoteException, InvalidTransactionException, TransactionAbortedException
	{
		checkValid(xid);
		cancelTimer(xid);
		try{
			if(lockManager.Lock(xid, "flight-"+flightNum, TransactionLockObject.LockType.LOCK_READ)){
				resetTimer(xid);
				return Middleware.m_flightsManager.queryFlightPrice(xid, flightNum);
			}
			else{
				//lockManager.Lock returns false for invalid parameters
				Trace.info("Locking on flight-" + flightNum + " failed: invalid parameters.");
			}

		}catch(DeadlockException e){
			abort(xid);
			abortedTransactions.add(xid);
			return -1;
		}
		resetTimer(xid);
		return -1;
	}

	// signature: Adrian Koretski
	public int queryCars(int xid, String location) throws RemoteException, InvalidTransactionException, TransactionAbortedException
	{
		checkValid(xid);
		cancelTimer(xid);
		
		try{
			if(lockManager.Lock(xid, "car-"+location, TransactionLockObject.LockType.LOCK_READ))
			{
				resetTimer(xid);
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
		resetTimer(xid);
		return -1;
	}
	

	// signature: Adrian Koretski
	public int queryCarsPrice(int xid, String location) throws RemoteException, InvalidTransactionException, TransactionAbortedException
	{
		checkValid(xid);
		cancelTimer(xid);
	
		try{
			if(lockManager.Lock(xid, "car-"+location, TransactionLockObject.LockType.LOCK_READ))
			{
				resetTimer(xid);
				return Middleware.m_carsManager.queryCarsPrice(xid, location);
			}
			else
			{
				Trace.info("Locking on car-" + location + " failed: invalid parameters.");
			}
		}catch(DeadlockException e){
			abort(xid);
			abortedTransactions.add(xid);
			return -1;
		}
		resetTimer(xid);
		return -1;
	}

	// signature: Adrian Koretski
	public int queryRooms(int xid, String location) throws RemoteException, InvalidTransactionException, TransactionAbortedException
	{
		checkValid(xid);
		cancelTimer(xid);
		
		try
		{
			if(lockManager.Lock(xid, "room-"+location, TransactionLockObject.LockType.LOCK_READ))
			{
				resetTimer(xid);
				return Middleware.m_carsManager.queryRooms(xid, location);
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
		resetTimer(xid);
		return -1;
	}

	public int queryRoomsPrice(int xid, String location) throws RemoteException, InvalidTransactionException, TransactionAbortedException
	{
		checkValid(xid);
		cancelTimer(xid);
		
		try{
			if(lockManager.Lock(xid, "room-"+location, TransactionLockObject.LockType.LOCK_READ))
			{
				resetTimer(xid);
				return Middleware.m_roomsManager.queryRoomsPrice(xid, location);
			}
			else
			{
				Trace.info("Locking on car-" + location + " failed: invalid parameters.");
			}
		}catch(DeadlockException e){
			abort(xid);
			abortedTransactions.add(xid);
			return -1;
		}
		resetTimer(xid);
		return -1;
	}
	
	public String queryCustomerInfo(int xid, int cid) throws RemoteException, InvalidTransactionException, TransactionAbortedException
	{
		checkValid(xid);
		cancelTimer(xid);
		
		String str = "customer-" + cid;
		try {
			if (lockManager.Lock(xid, str, TransactionLockObject.LockType.LOCK_READ))
			{
				if (toDeleteMap.get(xid).contains(str)) {
					Trace.info("Customer " + cid +" was deleted and cannot be accessed.");
					resetTimer(xid);
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
							return null;
						}
						else
						{
							writeDataCopy(xid, r_customer.getKey(), r_customer);
							transactionMap.get(xid).add(r_customer.getKey());
							resetTimer(xid);
							return r_customer.getBill();
						}
					}
					else 
					{
						writeDataCopy(xid, customer.getKey(), customer);
						transactionMap.get(xid).add(customer.getKey());
						resetTimer(xid);
						return customer.getBill();
					}
				}
			}
		}
		catch (DeadlockException e) {
			abort(xid);
			abortedTransactions.add(xid);
			return null;
		}
				
		resetTimer(xid);
		return null;
	}
	
	//Milestone 3
	//updated
	public boolean reserveFlight(int xid, int customerID, int flightNum) throws RemoteException, InvalidTransactionException, TransactionAbortedException{
		checkValid(xid);
		cancelTimer(xid);
		try{
			if(lockManager.Lock(xid, "customer-"+customerID, TransactionLockObject.LockType.LOCK_WRITE) && lockManager.Lock(xid, "flight-"+flightNum, TransactionLockObject.LockType.LOCK_WRITE)){
				
				//if the customer was deleted, should not be able to access customer or reserve the flight
				if(toDeleteMap.get(xid).contains("customer-"+customerID)){
					Trace.info("This customer: "+ "customer-"+customerID +" was deleted and cannot be accessed.");
					resetTimer(xid);
					return false;
				}

				//check location of customer, if it doesnt exist return false and trace 
				Customer curCustomer = (Customer) readDataCopy(xid, "customer-"+customerID);
				if(curCustomer == null){
					Customer remoteCustomer = (Customer) customersManager.getItem(xid, "customer-"+customerID);
					if(remoteCustomer == null){
						Trace.warn("customer does not exist in the local copy or in the remote server. Failed reservation.");
						resetTimer(xid);
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
					return false;
				}

				Customer finalCustomer = (Customer) readDataCopy(xid, "customer-"+customerID);

				Flight finalItem = new Flight(flightNum, Middleware.m_flightsManager.queryFlight(xid, flightNum), Middleware.m_flightsManager.queryFlightPrice(xid, flightNum));

				finalCustomer.reserve(finalItem.getKey(), String.valueOf(flightNum), finalItem.getPrice());
				writeDataCopy(xid, finalCustomer.getKey(), finalCustomer);

				Trace.info("Reservation succeeded:" +finalItem.getKey()+finalCustomer.getKey());
				resetTimer(xid);
				return true;
			}
		}catch(DeadlockException e){
			abort(xid);
			abortedTransactions.add(xid);
			return false;
		}
		resetTimer(xid);
		return false;

	}

	//milestone 3
	//update in progress
	//check reserveflight comments for info
	public boolean reserveCar(int xid, int customerID, String location) throws RemoteException, InvalidTransactionException, TransactionAbortedException
	{
		checkValid(xid);
		cancelTimer(xid);
		try{
			if(lockManager.Lock(xid, "customer-"+customerID, TransactionLockObject.LockType.LOCK_WRITE) && lockManager.Lock(xid, "car-"+location, TransactionLockObject.LockType.LOCK_WRITE)){
				if(toDeleteMap.get(xid).contains("car-"+location) || toDeleteMap.get(xid).contains("customer-"+customerID)){
					Trace.info("This car or customer was deleted and cannot be accessed.");
					resetTimer(xid);
					return false;
				}

				Customer curCustomer = (Customer) readDataCopy(xid, "customer-"+customerID);
				if(curCustomer == null){
					Customer remoteCustomer = (Customer) customersManager.getItem(xid, "customer-"+customerID);
					if(remoteCustomer == null){
						Trace.warn("customer does not exist in the local copy or in the remote server. Failed reservation.");
						resetTimer(xid);
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
					return false;
				}

				Customer finalCustomer = (Customer) readDataCopy(xid, "customer-"+customerID);

				Car finalItem = new Car(location, Middleware.m_carsManager.queryCars(xid, location), Middleware.m_carsManager.queryCarsPrice(xid, location));

				finalCustomer.reserve("car-"+location, location, finalItem.getPrice());
				writeDataCopy(xid, finalCustomer.getKey(), finalCustomer);

				Trace.info("Reservation succeeded:" +finalItem.getKey()+finalCustomer.getKey());
				resetTimer(xid);
				return true;

			}
		}catch(DeadlockException e){
			abort(xid);
			abortedTransactions.add(xid);
			return false;
		}
		resetTimer(xid);
		return false;
	}

	public boolean reserveRoom(int xid, int customerID, String location) throws RemoteException, InvalidTransactionException, TransactionAbortedException
	{
		checkValid(xid);
		cancelTimer(xid);
		try{
			if(lockManager.Lock(xid, "customer-"+customerID, TransactionLockObject.LockType.LOCK_WRITE) && lockManager.Lock(xid, "room-"+location, TransactionLockObject.LockType.LOCK_WRITE)){
				if(toDeleteMap.get(xid).contains("room-"+location) || toDeleteMap.get(xid).contains("customer-"+customerID)){
					Trace.info("This room or customer was deleted and cannot be accessed.");
					resetTimer(xid);
					return false;
				}

				Customer curCustomer = (Customer) readDataCopy(xid, "customer-"+customerID);
				if(curCustomer == null){
					Customer remoteCustomer = (Customer) customersManager.getItem(xid, "customer-"+customerID);
					if(remoteCustomer == null){
						Trace.warn("customer does not exist in the local copy or in the remote server. Failed reservation.");
						resetTimer(xid);
						return false;
					}
					else{
						writeDataCopy(xid, remoteCustomer.getKey(), remoteCustomer);
						transactionMap.get(xid).add(remoteCustomer.getKey());
					}
				}

				if(!Middleware.m_roomsManager.reserveRoom(xid, customerID, location)){
					Trace.warn("Rooms: "+ "room-"+location+" does not exist or it is full. Reservation failed.");
					resetTimer(xid);
					return false;
				}


				Customer finalCustomer = (Customer) readDataCopy(xid, "customer-"+customerID);

				Room finalItem = new Room(location, Middleware.m_roomsManager.queryRooms(xid, location), Middleware.m_roomsManager.queryRoomsPrice(xid, location));

				finalCustomer.reserve("room-"+location, location, finalItem.getPrice());
				writeDataCopy(xid, finalCustomer.getKey(), finalCustomer);

				
				Trace.info("Reservation succeeded:" +finalItem.getKey()+finalCustomer.getKey());
				
				resetTimer(xid);
				return true;

			}

		}catch(DeadlockException e){
			abort(xid);
			abortedTransactions.add(xid);
			return false;
		}
		resetTimer(xid);
		return false;
	}

	public boolean deleteFlight(int xid, int flightNum) throws RemoteException, InvalidTransactionException, TransactionAbortedException
	{
		checkValid(xid);
		cancelTimer(xid);
		
		try{
			if(lockManager.Lock(xid, "flight-"+flightNum, TransactionLockObject.LockType.LOCK_WRITE))
			{
				return Middleware.m_flightsManager.deleteFlight(xid, flightNum);
			}
			else {
				// Invalid Locking parameters.
			}


		}catch(DeadlockException e){
			abort(xid);
			abortedTransactions.add(xid);
			return false;
		}
		resetTimer(xid);
		return false;
	}

	public boolean deleteCars(int xid, String location) throws RemoteException, InvalidTransactionException, TransactionAbortedException
	{
		checkValid(xid);
		cancelTimer(xid);
		
		boolean inRM = false;
		try{
			if(lockManager.Lock(xid, "car-"+location, TransactionLockObject.LockType.LOCK_WRITE)){
				Car curObj = (Car) readDataCopy(xid, "car-"+location);
				if(curObj==null){
					Car remoteObj = (Car) Middleware.m_carsManager.getItem(xid, "car-"+location);
					if(remoteObj==null){
						Trace.warn("The item does not exist and thus cannot be deleted.");
						resetTimer(xid);
						return false;
					}
					else{
						if(remoteObj.getReserved()==0){
							toDeleteMap.get(xid).add(remoteObj.getKey());
							transactionMap.get(xid).remove(remoteObj.getKey());
							Trace.info("Cars: " + remoteObj.getKey()+" is added to delete map.");
							resetTimer(xid);
							return true;
						}
						else{
							Trace.info("Cars: "+ remoteObj.getKey()+" cannot be deleted because someone has reserved it.");
							resetTimer(xid);
							return false;
						}
					}
				}
				else
				{
					Car remoteObj = (Car) Middleware.m_carsManager.getItem(xid, "car-"+location);
					if(remoteObj!=null){
						inRM = true;
					}
					if(curObj.getReserved()==0){
						if (inRM) {
							System.out.println(curObj.getKey() + " is both in LC and RM");
							toDeleteMap.get(xid).add(curObj.getKey());
						}							
						transactionMap.get(xid).remove(curObj.getKey());
						m_dataCopy.remove(curObj.getKey());
						Trace.info("Cars: " + curObj.getKey()+" is added to delete map.");
						resetTimer(xid);
						return true;
					}else{
						Trace.info("Cars: "+ curObj.getKey()+" cannot be deleted because someone has reserved it.");
						resetTimer(xid);
						return false;
					}
				}
			}
		}catch(DeadlockException e){
			abort(xid);
			abortedTransactions.add(xid);
			return false;
		}
		resetTimer(xid);
		return false;
	}

	public boolean deleteRooms(int xid, String location) throws RemoteException, InvalidTransactionException, TransactionAbortedException
	{
		checkValid(xid);
		cancelTimer(xid);
		
		boolean inRM = false;
		
		try{
			if(lockManager.Lock(xid, "room-"+location, TransactionLockObject.LockType.LOCK_WRITE)){
				Room curObj = (Room) readDataCopy(xid, "room-"+location);
				if(curObj==null){
					Room remoteObj = (Room) Middleware.m_roomsManager.getItem(xid, "room-"+location);
					if(remoteObj == null){
						Trace.warn("The item does not exist and thus cannot be deleted.");
						resetTimer(xid);
						return false;
					}
					else{
						if(remoteObj.getReserved()==0){
							toDeleteMap.get(xid).add(remoteObj.getKey());
							transactionMap.get(xid).remove(remoteObj.getKey());
							Trace.info("Rooms: " + remoteObj.getKey()+" is added to delete map.");
							resetTimer(xid);
							return true;
						}
						else{
							Trace.info("Rooms: "+ remoteObj.getKey()+" cannot be deleted because someone has reserved it.");
							resetTimer(xid);
							return false;
						}
					}
				}
				else
				{
					Room remoteObj = (Room) Middleware.m_roomsManager.getItem(xid, "room-"+location);
					if(remoteObj != null){
						inRM = true;
					}
					if(curObj.getReserved()==0)
					{
						if (inRM) {
							toDeleteMap.get(xid).add(curObj.getKey());							
						}
						transactionMap.get(xid).remove(curObj.getKey());
						m_dataCopy.remove(curObj.getKey());
						Trace.info("Rooms: " + curObj.getKey()+" is added to delete map.");
						resetTimer(xid);
						return true;
					}else{
						Trace.info("Rooms: "+ curObj.getKey()+" cannot be deleted because someone has reserved it.");
						resetTimer(xid);
						return false;
					}
				}
			}

		}catch(DeadlockException e){
			abort(xid);
			abortedTransactions.add(xid);
			return false;
		}
		resetTimer(xid);
		return false;
	}


	//Milestone 3 update in progress
	public boolean deleteCustomer(int xid, int customerID) throws RemoteException, InvalidTransactionException, TransactionAbortedException
	{
		checkValid(xid);
		cancelTimer(xid);
		
		
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
				return true;
			}
		}catch(DeadlockException e){
			abort(xid);
			abortedTransactions.add(xid);
			return false;
		}
		resetTimer(xid);
		return false;
	}
	
	//TODO Query everything first so it is all or nothing
	
	public boolean bundle(int xid, int customerID, Vector<String> flightNumbers, String location, boolean car, boolean room) throws RemoteException, InvalidTransactionException, TransactionAbortedException 
	{
		checkValid(xid);
		cancelTimer(xid);
		
		String str = "customer-" + customerID;
		
		// Attempt to query flights, cars and rooms before locking on customer and return false if one doesn't exist
		for (String num : flightNumbers) 
		{
			try {
				if(lockManager.Lock(xid, "flight-"+num, TransactionLockObject.LockType.LOCK_READ)){
					
					if(queryFlightBundle(xid, Integer.parseInt(num))==-1){
						Trace.info("flight does not exist abort bundle.");
						resetTimer(xid);
						return false;
					}
				}
				else {
					System.out.println("Could not get read lock on flights");
				}
			} catch (Exception e) {
				e.printStackTrace();			}
		}
		
		try {
			if(car && lockManager.Lock(xid, "car-"+location, TransactionLockObject.LockType.LOCK_READ))
			{
				if(queryCarsBundle(xid, location) == -1){
					Trace.info("car at this location does not exist. abort bundle.");
					resetTimer(xid);
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
					if(remoteObj == null){
						Trace.warn("Customer " + customerID + " does not exist. Bundle failed.");
						resetTimer(xid);
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
			return false;
		}
		
		resetTimer(xid);
		return true;
	}
	
	// utility function for bundle to call
	// No use of timers else we would get some lost Timer threads running around leading to transaction timeouts after commiting (not fun)
	private int queryCarsBundle(int xid, String location) throws RemoteException, InvalidTransactionException, TransactionAbortedException
	{
		checkValid(xid);
		
		try{
			if(lockManager.Lock(xid, "car-"+location, TransactionLockObject.LockType.LOCK_READ)){
				if(toDeleteMap.get(xid).contains("car-"+location)){
					Trace.info("This car: "+ "car-"+location +" was deleted and cannot be accessed.");
					return -1;
				}
				Car curObj = (Car)readDataCopy(xid, "car-"+location);
				if(curObj==null){
					Car remoteObj = (Car) Middleware.m_carsManager.getItem(xid, "car-"+location);
					if(remoteObj==null){
						Trace.info("Cannot find cars at the location asked.");
						return -1;
					}
					else{
						writeDataCopy(xid, remoteObj.getKey(), remoteObj);
						transactionMap.get(xid).add(remoteObj.getKey());
						return (remoteObj.getCount());
					}
				}
				else{
					return (curObj.getCount());
				}
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
			if(lockManager.Lock(xid, "flight-"+flightNum, TransactionLockObject.LockType.LOCK_READ)){
				Flight curObj = (Flight)readDataCopy(xid, "flight-"+flightNum);
				if(curObj==null){
					Flight remoteObj = (Flight) Middleware.m_flightsManager.getItem(xid, "flight-"+flightNum);
					if(remoteObj!=null){
						writeDataCopy(xid, remoteObj.getKey(), remoteObj);
						transactionMap.get(xid).add(remoteObj.getKey());
						return (remoteObj.getCount());
					}
					else{
						Trace.info("Cannot find the flight with the flight number asked.");
						return -1;
					}
				}
				else{
					return (curObj.getCount());
				}
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
		// Does not act upon Timer because it would lead to having multiple Timer threads for the single objects and some bugs where the transaction committed
		// successfully but there would still be a timer for it running somewhere.
		try{
			if(lockManager.Lock(xid, "customer-"+customerID, TransactionLockObject.LockType.LOCK_WRITE) && lockManager.Lock(xid, "flight-"+flightNum, TransactionLockObject.LockType.LOCK_WRITE)){
				if(toDeleteMap.get(xid).contains("flight-"+flightNum) || toDeleteMap.get(xid).contains("customer-"+customerID)){
					Trace.info("This flight: "+ "flight-"+flightNum +" was deleted and cannot be accessed.");
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
				Flight item = (Flight) readDataCopy(xid, "flight-"+flightNum);
				if(item == null){
					Flight remoteItem = (Flight) Middleware.m_flightsManager.getItem(xid, "flight-"+flightNum);
					if(remoteItem == null){
						Trace.warn("The item we are trying to reserve does not exist in the local copy or in the remote Server. Failed reservation.");
						return false;
					}
					else{
						writeDataCopy(xid, remoteItem.getKey(), remoteItem);
						transactionMap.get(xid).add(remoteItem.getKey());
					}
				}

				Flight finalItem = (Flight) readDataCopy(xid, "flight-"+flightNum);
				if(finalItem.getCount() == 0){
					Trace.warn("The item we are trying to reserve has no more space available.");
					return false;
				}

				Customer finalCustomer = (Customer) readDataCopy(xid, "customer-"+customerID);

				finalCustomer.reserve("flight-"+flightNum, String.valueOf(flightNum), finalItem.getPrice());
				writeDataCopy(xid, finalCustomer.getKey(), finalCustomer);

				finalItem.setCount(finalItem.getCount() - 1);
				finalItem.setReserved(finalItem.getReserved() + 1);
				writeDataCopy(xid, finalItem.getKey(), finalItem);
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
			if(lockManager.Lock(xid, "room-"+location, TransactionLockObject.LockType.LOCK_READ)){
				if(toDeleteMap.get(xid).contains("room-"+location)){
					Trace.info("This room: "+ "room-"+location +" was deleted and cannot be accessed.");
					return -1;
				}

				Room curObj = (Room) readDataCopy(xid, "room-"+location);
				if(curObj == null){
					Room remoteObj = (Room) Middleware.m_roomsManager.getItem(xid, "room-"+location);
					if(remoteObj == null){
						Trace.info("Cannot find rooms at this location");
						return -1;
					}
					else{
						writeDataCopy(xid, remoteObj.getKey(), remoteObj);
						transactionMap.get(xid).add(remoteObj.getKey());
						return (remoteObj.getCount());
					}
				}
				else{
					return(curObj.getCount());
				}
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
		// Does not act upon Timer because it would lead to having multiple Timer threads for the single objects and some bugs where the transaction committed
		// successfully but there would still be a timer for it running somewhere.
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
						writeDataCopy(xid, remoteCustomer.getKey(), remoteCustomer);
						transactionMap.get(xid).add(remoteCustomer.getKey());
					}
				}

				Room item = (Room) readDataCopy(xid, "room-"+location);
				if(item==null){
					Room remoteItem = (Room) Middleware.m_roomsManager.getItem(xid, "room-"+location);
					if(remoteItem==null){
						Trace.warn("The item we are trying to reserve does not exist in the local copy or in the remote Server. Failed reservation.");
						return false;
					}
					else{
						writeDataCopy(xid, remoteItem.getKey(), remoteItem);
						transactionMap.get(xid).add(remoteItem.getKey());
					}
				}

				Room finalItem = (Room) readDataCopy(xid, "room-"+location);
				if(finalItem.getCount() == 0){
					Trace.warn("The item we are trying to reserve has no more space available.");
					return false;
				}

				Customer finalCustomer = (Customer) readDataCopy(xid, "customer-"+customerID);

				finalCustomer.reserve("room-"+location, location, finalItem.getPrice());
				writeDataCopy(xid, finalCustomer.getKey(), finalCustomer);

				finalItem.setCount(finalItem.getCount() - 1);
				finalItem.setReserved(finalItem.getReserved() + 1);
				writeDataCopy(xid, finalItem.getKey(), finalItem);
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
		// Does not act upon Timer because it would lead to having multiple Timer threads for the single objects and some bugs where the transaction committed
		// successfully but there would still be a timer for it running somewhere.
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

				Car item = (Car) readDataCopy(xid, "car-"+location);
				if(item==null){
					Car remoteItem = (Car) Middleware.m_carsManager.getItem(xid, "car-"+location);
					if(remoteItem==null){
						Trace.warn("The item we are trying to reserve does not exist in the local copy or in the remote Server. Failed reservation.");

						return false;
					}
					else{
						writeDataCopy(xid, remoteItem.getKey(), remoteItem);
						transactionMap.get(xid).add(remoteItem.getKey());
					}
				}

				Car finalItem = (Car) readDataCopy(xid, "car-"+location);
				if(finalItem.getCount() == 0){
					Trace.warn("The item we are trying to reserve has no more space available.");

					return false;
				}

				Customer finalCustomer = (Customer) readDataCopy(xid, "customer-"+customerID);

				finalCustomer.reserve("car-"+location, location, finalItem.getPrice());
				writeDataCopy(xid, finalCustomer.getKey(), finalCustomer);

				finalItem.setCount(finalItem.getCount() - 1);
				finalItem.setReserved(finalItem.getReserved() + 1);
				writeDataCopy(xid, finalItem.getKey(), finalItem);
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
		if (!transactions.contains((Integer) xid)) {
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
	

	
	public static void main(String[] args) {
		
		Middleware middleware = new RMIMiddleware("Middleware");
		
		middleware.m_flightsManager = new ResourceManager("Flights");
		middleware.m_carsManager = new ResourceManager("Cars");
		middleware.m_roomsManager = new ResourceManager("Rooms");
		
		TransactionManager TM = new TransactionManager(middleware);
		
		try {
			TM.test3();
		} 
		catch (Exception e) {
			e.printStackTrace();
		}

	}
	
	private void test3() throws RemoteException, InvalidTransactionException, TransactionAbortedException {
		
		int xid = start();
		
		addFlight(xid, 1, 50, 100);
		deleteFlight(xid, 1);
		
		addCars(xid, "mtl", 10, 20);
		deleteCars(xid, "mtl");
		
		addRooms(xid, "mtl", 20, 50);
		deleteRooms(xid, "mtl");
		
		commit(xid);
		System.out.println();
		
		xid = start();
		
		newCustomer(xid, 1);
		
		addFlight(xid, 1, 50, 100);
		reserveFlight(xid, 1, 1);
		
		deleteCustomer(xid, 1);
		commit(xid);
		
		if ( ((Flight)Middleware.m_flightsManager.getItem(xid, "flight-1")).getCount() == 50) {
			System.out.println("SUCCESS");
		}
		
	}
	
	
	
	private void test2() throws RemoteException, InvalidTransactionException, TransactionAbortedException {
		int xid = start();
		int cid = 1;
		
		addFlight(xid, 1, 50, 100);
		addFlight(xid, 2, 40, 200);
		addFlight(xid, 3, 60, 150);
		
		newCustomer(xid, cid);
		
		addCars(xid, "mtl", 10, 50);
		addRooms(xid, "ottawa", 45, 500);
		
		commit(xid);
		
		xid = start();
		Vector<String> args = new Vector<>();
		args.add("1");
		args.add("2");
		args.add("3");
		
		try {
			bundle(xid, cid, args, "mtl", true, true);
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		commit(xid);
		System.out.println(transactionMap.get(xid));
	}
	
	
	private void test1() throws InvalidTransactionException, RemoteException, TransactionAbortedException {
		System.out.println("");
		int xid = start();
		addFlight(xid, 1, 50, 100);
		commit(xid);
		
		if (Middleware.m_flightsManager.queryFlight(xid, 1) == 50) {
			System.out.println("SUCCESS");
		}
		
		
		
	}

}