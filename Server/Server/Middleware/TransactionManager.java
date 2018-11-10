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

	private Map<Integer, ArrayList<String>> txMap = new HashMap<>();
	private Map<Integer, ArrayList<String>> toDeleteMap = new HashMap<>();
	
	private Map<Integer, Timer> transactionTimers = new HashMap<Integer, Timer>();
	
	private IResourceManager customersManager = null;
	

	public TransactionManager(IResourceManager custs)
	{
		xidCounter = 0;
		this.customersManager = custs;

	}
	

	public int start() {
		int xid = ++xidCounter;
		transactions.add(xid);
		txMap.put(xid, new ArrayList<String>());
		toDeleteMap.put(xid, new ArrayList<String>());
		
		Timer timer = new Timer();
		timer.schedule(new TimerTask() {
			@Override
			public void run() {
				Trace.info("Transaction " + xid + " timed out");
				try {
					abort(xid);
				} catch (InvalidTransactionException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
				abortedTransactions.add(xid);
			}
		}, TTL);
		
		transactionTimers.put(xid, timer);
		Trace.info("TM::start() Transaction " + xid + " started");
		
		return xid;
	}
	
	public void abort(int xid) throws InvalidTransactionException {
		if (!transactions.contains((Integer) xid)) {
			//System.out.println("transactions does not contain tx");
			throw new InvalidTransactionException(xid, null);
		}
		else
		{
			//System.out.println("transaction does contain tx");
			transactionTimers.get(xid).cancel();
			transactionTimers.remove((Integer) xid);
			
			for (String s : txMap.get(xid)) 
			{
				m_dataCopy.remove(s);
			}
			txMap.remove(xid);
			transactions.remove((Integer) xid);
			toDeleteMap.remove(xid);
			lockManager.UnlockAll(xid);
			
			System.out.println("From TM: " + xid + " ABORTED");
		}
	}



	public boolean shutdown() throws RemoteException{
		return false;
	}

	public boolean commit(int transactionId) throws RemoteException, InvalidTransactionException, TransactionAbortedException{
		checkValid(transactionId);
		cancelTimer(transactionId);

		for(String s : txMap.get(transactionId)){
			RMItem toSend = readDataCopy(transactionId, s);
			Trace.info(s);
			if(toSend == null){
				break;
			}
			if(s.charAt(0) == 'f'){
				Middleware.m_flightsManager.putItem(transactionId, ((Flight) toSend).getKey(), toSend);
			}
			else if(s.charAt(0) == 'c' && s.charAt(1) == 'a'){
				Middleware.m_carsManager.putItem(transactionId, ((Car) toSend).getKey(), toSend);
			}
			else if(s.charAt(0) == 'r'){
				Middleware.m_roomsManager.putItem(transactionId, ((Room) toSend).getKey(), toSend);
			}
			else if(s.charAt(0) == 'c' && s.charAt(1) == 'u'){
				customersManager.putItem(transactionId, ((Customer)toSend).getKey(), toSend);
			}	
			m_dataCopy.remove(s);
		}

		//need deleteItem
		for(String s : toDeleteMap.get(transactionId))
		{
			System.out.println("Iterating through deleteMap:\nCurrent key: " + s);
			
//			RMItem toDelete = readDataCopy(transactionId, s);
			if (s == null) {
				break;
			}
			if(s.charAt(0) == 'f'){
				System.out.println("@commit time: flight found in toDeleteMap");
				Middleware.m_flightsManager.deleteData(transactionId, s);
			}
			else if(s.charAt(0) == 'c' && s.charAt(1) == 'a'){
				Middleware.m_carsManager.deleteData(transactionId, s);
			}
			else if(s.charAt(0) == 'r'){
				Middleware.m_roomsManager.deleteData(transactionId, s);
			}
			else if(s.charAt(0) == 'c' && s.charAt(1) == 'u'){
				System.out.println("attempt to delete customer at RM");
				customersManager.deleteData(transactionId, s);
			}	
			m_dataCopy.remove(s);
		}
		
		transactionTimers.remove((Integer) transactionId);
		transactions.remove((Integer) transactionId);
		txMap.remove(transactionId);
		toDeleteMap.remove(transactionId);
		lockManager.UnlockAll(transactionId);
		
		System.out.println("Transaction: " + transactionId + " has commited");
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



	public boolean addFlight(int xid, int flightNum, int flightSeats, int flightPrice) throws RemoteException, InvalidTransactionException, TransactionAbortedException
	{
		checkValid(xid);
		cancelTimer(xid);
		try{
			if(toDeleteMap.get(xid).contains("flight-"+flightNum)){
				toDeleteMap.get(xid).remove("flight-"+flightNum);
			}
			if(lockManager.Lock(xid, "flight-"+flightNum, TransactionLockObject.LockType.LOCK_WRITE)){
				Flight curObj = (Flight)readDataCopy(xid, Flight.getKey(flightNum));
				if(curObj == null){
					
					Flight remoteObj = (Flight) Middleware.m_flightsManager.getItem(xid, "flight-"+flightNum);

					if(remoteObj!=null){
						remoteObj.setCount(remoteObj.getCount()+flightSeats);
						if(flightPrice>0) remoteObj.setPrice(flightPrice);
						writeDataCopy(xid, remoteObj.getKey(), remoteObj);
						txMap.get(xid).add(remoteObj.getKey());
						Trace.info("Local copy write addFlight successful.");
					}
					else{
						Flight newObj = new Flight(flightNum, flightSeats, flightPrice);
						writeDataCopy(xid, newObj.getKey(), newObj);
						txMap.get(xid).add(newObj.getKey());
						Trace.info("Local copy added and write addFlight successful.");
					}
				}
				else{
					curObj.setCount(curObj.getCount() + flightSeats);
					if (flightPrice > 0){
						curObj.setPrice(flightPrice);
					}
					writeDataCopy(xid, curObj.getKey(), curObj);
					Trace.info("Local copy added and write addFlight successful.");
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

	public boolean addCars(int xid, String location, int count, int price) throws RemoteException, InvalidTransactionException, TransactionAbortedException
	{
		checkValid(xid);
		cancelTimer(xid);
		try{
			if(toDeleteMap.get(xid).contains("car-"+location)){
				toDeleteMap.get(xid).remove("car-"+location);
			}

			if(lockManager.Lock(xid, "car-"+location, TransactionLockObject.LockType.LOCK_WRITE)){
				Car curObj = (Car) readDataCopy(xid, Car.getKey(location));
				
				if(curObj == null)
				{
					Car remoteObj = (Car) Middleware.m_carsManager.getItem(xid, "car-"+location);
					if(remoteObj !=null)
					{
						remoteObj.setCount(remoteObj.getCount()+count);
						if(price>0) remoteObj.setPrice(price);
						writeDataCopy(xid, remoteObj.getKey(), remoteObj);
						txMap.get(xid).add(remoteObj.getKey());
						Trace.info("Local copy write addCars successful.");
					}
					else{
						Car newObj = new Car(location, count, price);
						writeDataCopy(xid, newObj.getKey(), newObj);
						txMap.get(xid).add(newObj.getKey());
						Trace.info("Local copy added and write addCars successful first.");
					}
				}
				else{
					curObj.setCount(curObj.getCount()+count);
					if(price>0) curObj.setPrice(price);
					writeDataCopy(xid, curObj.getKey(), curObj);
					Trace.info("Local copy added and write addCars successful.");
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

	public boolean addRooms(int xid, String location, int count, int price) throws RemoteException, InvalidTransactionException, TransactionAbortedException
	{
		checkValid(xid);
		cancelTimer(xid);
		try{
			if(toDeleteMap.get(xid).contains("room-"+location)){
				toDeleteMap.get(xid).remove("room-"+location);
			}

			if(lockManager.Lock(xid, "room-"+location, TransactionLockObject.LockType.LOCK_WRITE)){
				Room curObj = (Room) readDataCopy(xid, Room.getKey(location));
				if(curObj==null){
					Room remoteObj = (Room) Middleware.m_roomsManager.getItem(xid, "room-"+location);
					if(remoteObj!=null){
						remoteObj.setCount(remoteObj.getCount()+count);
						if(price>0) remoteObj.setCount(price);
						writeDataCopy(xid, remoteObj.getKey(), remoteObj);
						txMap.get(xid).add(remoteObj.getKey());
						Trace.info("Local copy addRooms successful");
					}
					else{
						Room newObj = new Room(location, count, price);
						writeDataCopy(xid, newObj.getKey(), newObj);
						txMap.get(xid).add(newObj.getKey());
						Trace.info("Local copy added and write addRooms successful.");
					}
				}
				else{
					curObj.setCount(curObj.getCount()+count);
					if(price>0) curObj.setPrice(price);
					writeDataCopy(xid, curObj.getKey(), curObj);
					Trace.info("Local copy added and write addRooms successful.");
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
				txMap.get(xid).add(customer.getKey());
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
						txMap.get(xid).add(newCustomer.getKey());
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

	public int queryFlight(int xid, int flightNum) throws RemoteException, InvalidTransactionException, TransactionAbortedException
	{
		checkValid(xid);
		cancelTimer(xid);
		try{
			if(lockManager.Lock(xid, "flight-"+flightNum, TransactionLockObject.LockType.LOCK_READ)){
				Flight curObj = (Flight)readDataCopy(xid, "flight-"+flightNum);
				if(curObj==null){
					Flight remoteObj = (Flight) Middleware.m_flightsManager.getItem(xid, "flight-"+flightNum);
					if(remoteObj!=null){
						writeDataCopy(xid, remoteObj.getKey(), remoteObj);
						txMap.get(xid).add(remoteObj.getKey());
						resetTimer(xid);
						return (remoteObj.getCount());
					}
					else{
						Trace.info("Cannot find the flight with the flight number asked.");
						resetTimer(xid);
						return -1;
					}
				}
				else{
					resetTimer(xid);
					return (curObj.getCount());
				}
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

				if(toDeleteMap.get(xid).contains("flight-"+flightNum)){
					Trace.info("This flight: "+ "flight-"+flightNum +" was deleted and cannot be accessed.");
					resetTimer(xid);
					return -1;
				}
				Flight curObj = (Flight)readDataCopy(xid, "flight-"+flightNum);
				if(curObj==null){
					Flight remoteObj = (Flight) Middleware.m_flightsManager.getItem(xid, "flight-"+flightNum);
					if(remoteObj!=null){
						writeDataCopy(xid, remoteObj.getKey(), remoteObj);
						txMap.get(xid).add(remoteObj.getKey());
						resetTimer(xid);
						return (remoteObj.getPrice());
					}
					else{
						Trace.info("Cannot find the flight with the flight number asked.");
						resetTimer(xid);
						return -1;
					}
				}
				else{
					resetTimer(xid);
					return (curObj.getPrice());
				}
			}

		}catch(DeadlockException e){
			abort(xid);
			abortedTransactions.add(xid);
			return -1;
		}
		resetTimer(xid);
		return -1;
	}

	public int queryCars(int xid, String location) throws RemoteException, InvalidTransactionException, TransactionAbortedException
	{
		checkValid(xid);
		cancelTimer(xid);
		
		try{
			if(lockManager.Lock(xid, "car-"+location, TransactionLockObject.LockType.LOCK_READ)){
				if(toDeleteMap.get(xid).contains("car-"+location)){
					Trace.info("This car: "+ "car-"+location +" was deleted and cannot be accessed.");
					resetTimer(xid);
					return -1;
				}
				Car curObj = (Car)readDataCopy(xid, "car-"+location);
				if(curObj==null){
					Car remoteObj = (Car) Middleware.m_carsManager.getItem(xid, "car-"+location);
					if(remoteObj==null){
						Trace.info("Cannot find cars at the location asked.");
						resetTimer(xid);
						return -1;
					}
					else{
						writeDataCopy(xid, remoteObj.getKey(), remoteObj);
						txMap.get(xid).add(remoteObj.getKey());
						resetTimer(xid);
						return (remoteObj.getCount());
					}
				}
				else{
					resetTimer(xid);
					return (curObj.getCount());
				}
			}
		}catch(DeadlockException e){
			abort(xid);
			abortedTransactions.add(xid);
			return -1;
		}
		resetTimer(xid);
		return -1;
	}
	


	public int queryCarsPrice(int xid, String location) throws RemoteException, InvalidTransactionException, TransactionAbortedException
	{
		checkValid(xid);
		cancelTimer(xid);
	
		try{
			if(lockManager.Lock(xid, "car-"+location, TransactionLockObject.LockType.LOCK_READ)){
				if(toDeleteMap.get(xid).contains("car-"+location)){
					Trace.info("This car: "+ "car-"+location +" was deleted and cannot be accessed.");
					resetTimer(xid);
					return -1;
				}
				Car curObj = (Car)readDataCopy(xid, "car-"+location);
				if(curObj==null){
					Car remoteObj = (Car) Middleware.m_carsManager.getItem(xid, "car-"+location);
					if(remoteObj==null){
						Trace.info("Cannot find cars at the location asked.");
						resetTimer(xid);
						return -1;
					}
					else{
						writeDataCopy(xid, remoteObj.getKey(), remoteObj);
						txMap.get(xid).add(remoteObj.getKey());
						resetTimer(xid);
						return (remoteObj.getPrice());
					}
				}
				else{
					resetTimer(xid);
					return (curObj.getPrice());
				}
			}
		}catch(DeadlockException e){
			abort(xid);
			abortedTransactions.add(xid);
			return -1;
		}
		resetTimer(xid);
		return -1;
	}



	public int queryRooms(int xid, String location) throws RemoteException, InvalidTransactionException, TransactionAbortedException
	{
		checkValid(xid);
		cancelTimer(xid);
		
		try {
			if(lockManager.Lock(xid, "room-"+location, TransactionLockObject.LockType.LOCK_READ)){
				if(toDeleteMap.get(xid).contains("room-"+location)){
					Trace.info("This room: "+ "room-"+location +" was deleted and cannot be accessed.");
					resetTimer(xid);
					return -1;
				}

				Room curObj = (Room) readDataCopy(xid, "room-"+location);
				if(curObj == null){
					Room remoteObj = (Room) Middleware.m_roomsManager.getItem(xid, "room-"+location);
					if(remoteObj == null){
						Trace.info("Cannot find rooms at this location");
						resetTimer(xid);
						return -1;
					}
					else{
						writeDataCopy(xid, remoteObj.getKey(), remoteObj);
						txMap.get(xid).add(remoteObj.getKey());
						resetTimer(xid);
						return (remoteObj.getCount());
					}
				}
				else{
					resetTimer(xid);
					return(curObj.getCount());
				}
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
			if(lockManager.Lock(xid, "room-"+location, TransactionLockObject.LockType.LOCK_READ)){
				if(toDeleteMap.get(xid).contains("room-"+location)){
					Trace.info("This room: "+ "room-"+location +" was deleted and cannot be accessed.");
					resetTimer(xid);
					return -1;
				}

				Room curObj = (Room) readDataCopy(xid, "room-"+location);
				if(curObj == null){
					Room remoteObj = (Room) Middleware.m_roomsManager.getItem(xid, "room-"+location);
					if(remoteObj == null){
						Trace.info("Cannot find rooms at this location");
						resetTimer(xid);
						return -1;
					}
					else{
						writeDataCopy(xid, remoteObj.getKey(), remoteObj);
						txMap.get(xid).add(remoteObj.getKey());
						resetTimer(xid);
						return (remoteObj.getPrice());
					}
				}
				else{
					resetTimer(xid);
					return(curObj.getPrice());
				}
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
							txMap.get(xid).add(r_customer.getKey());
							resetTimer(xid);
							return r_customer.getBill();
						}
					}
					else 
					{
						writeDataCopy(xid, customer.getKey(), customer);
						txMap.get(xid).add(customer.getKey());
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
	

	public boolean reserveFlight(int xid, int customerID, int flightNum) throws RemoteException, InvalidTransactionException, TransactionAbortedException{
		checkValid(xid);
		cancelTimer(xid);
		try{
			if(lockManager.Lock(xid, "customer-"+customerID, TransactionLockObject.LockType.LOCK_WRITE) && lockManager.Lock(xid, "flight-"+flightNum, TransactionLockObject.LockType.LOCK_WRITE)){
				if(toDeleteMap.get(xid).contains("flight-"+flightNum) || toDeleteMap.get(xid).contains("customer-"+customerID)){
					Trace.info("This flight: "+ "flight-"+flightNum +" was deleted and cannot be accessed.");
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
						txMap.get(xid).add(remoteCustomer.getKey());
					}
				}
				Flight item = (Flight) readDataCopy(xid, "flight-"+flightNum);
				if(item == null){
					Flight remoteItem = (Flight) Middleware.m_flightsManager.getItem(xid, "flight-"+flightNum);
					if(remoteItem == null){
						Trace.warn("The item we are trying to reserve does not exist in the local copy or in the remote Server. Failed reservation.");
						resetTimer(xid);
						return false;
					}
					else{
						writeDataCopy(xid, remoteItem.getKey(), remoteItem);
						txMap.get(xid).add(remoteItem.getKey());
					}
				}

				Flight finalItem = (Flight) readDataCopy(xid, "flight-"+flightNum);
				if(finalItem.getCount() == 0){
					Trace.warn("The item we are trying to reserve has no more space available.");
					resetTimer(xid);
					return false;
				}

				Customer finalCustomer = (Customer) readDataCopy(xid, "customer-"+customerID);

				finalCustomer.reserve("flight-"+flightNum, String.valueOf(flightNum), finalItem.getPrice());
				writeDataCopy(xid, finalCustomer.getKey(), finalCustomer);

				finalItem.setCount(finalItem.getCount() - 1);
				finalItem.setReserved(finalItem.getReserved() + 1);
				writeDataCopy(xid, finalItem.getKey(), finalItem);
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
						txMap.get(xid).add(remoteCustomer.getKey());
					}
				}

				Car item = (Car) readDataCopy(xid, "car-"+location);
				if(item==null){
					Car remoteItem = (Car) Middleware.m_carsManager.getItem(xid, "car-"+location);
					if(remoteItem==null){
						Trace.warn("The item we are trying to reserve does not exist in the local copy or in the remote Server. Failed reservation.");
						resetTimer(xid);
						return false;
					}
					else{
						writeDataCopy(xid, remoteItem.getKey(), remoteItem);
						txMap.get(xid).add(remoteItem.getKey());
					}
				}

				Car finalItem = (Car) readDataCopy(xid, "car-"+location);
				if(finalItem.getCount() == 0){
					Trace.warn("The item we are trying to reserve has no more space available.");
					resetTimer(xid);
					return false;
				}

				Customer finalCustomer = (Customer) readDataCopy(xid, "customer-"+customerID);

				finalCustomer.reserve("car-"+location, location, finalItem.getPrice());
				writeDataCopy(xid, finalCustomer.getKey(), finalCustomer);

				finalItem.setCount(finalItem.getCount() - 1);
				finalItem.setReserved(finalItem.getReserved() + 1);
				writeDataCopy(xid, finalItem.getKey(), finalItem);
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
						txMap.get(xid).add(remoteCustomer.getKey());
					}
				}

				Room item = (Room) readDataCopy(xid, "room-"+location);
				if(item==null){
					Room remoteItem = (Room) Middleware.m_roomsManager.getItem(xid, "room-"+location);
					if(remoteItem==null){
						Trace.warn("The item we are trying to reserve does not exist in the local copy or in the remote Server. Failed reservation.");
						resetTimer(xid);
						return false;
					}
					else{
						writeDataCopy(xid, remoteItem.getKey(), remoteItem);
						txMap.get(xid).add(remoteItem.getKey());
					}
				}

				Room finalItem = (Room) readDataCopy(xid, "room-"+location);
				if(finalItem.getCount() == 0){
					Trace.warn("The item we are trying to reserve has no more space available.");
					resetTimer(xid);
					return false;
				}

				Customer finalCustomer = (Customer) readDataCopy(xid, "customer-"+customerID);

				finalCustomer.reserve("room-"+location, location, finalItem.getPrice());
				writeDataCopy(xid, finalCustomer.getKey(), finalCustomer);

				finalItem.setCount(finalItem.getCount() - 1);
				finalItem.setReserved(finalItem.getReserved() + 1);
				writeDataCopy(xid, finalItem.getKey(), finalItem);
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
		
		boolean inRM = false;
		
		try{
			if(lockManager.Lock(xid, "flight-"+flightNum, TransactionLockObject.LockType.LOCK_WRITE))
			{
				Flight curObj = (Flight) readDataCopy(xid, "flight-"+flightNum);
				if(curObj == null)
				{
					Flight remoteObj = (Flight) Middleware.m_flightsManager.getItem(xid, "flight-"+flightNum);
					if(remoteObj == null)
					{
						Trace.warn("The item does not exist and thus cannot be deleted.");
						resetTimer(xid);
						return false;
					}
					else{
						if(remoteObj.getReserved()==0){
							inRM = true;
							System.out.println("flight was not in LC, was found in RM and is added to deletemap");
							toDeleteMap.get(xid).add(remoteObj.getKey());
							txMap.get(xid).remove(remoteObj.getKey());
							Trace.info("Flight: " + remoteObj.getKey()+" is added to delete map.");
							resetTimer(xid);
							return true;
						}
						else{
							Trace.info("Flight: "+ remoteObj.getKey()+" cannot be deleted because someone has reserved it.");
							resetTimer(xid);
							return false;
						}
					}
				}
				else
				{
					Flight remoteObj = (Flight) Middleware.m_flightsManager.getItem(xid, "flight-"+flightNum);
					if(remoteObj != null) {
						inRM = true;
					}
					
					if(curObj.getReserved()==0)
					{
						removeDataCopy(xid, curObj.getKey());
						
						if (inRM) {
							toDeleteMap.get(xid).add(curObj.getKey());							
						}
						txMap.get(xid).remove(curObj.getKey());
						m_dataCopy.remove(curObj.getKey());
						Trace.info("Flight: " + curObj.getKey()+" is added to delete map and was deleted from local copy.");
						resetTimer(xid);
						return true;
					}
					else{
						Trace.info("Flight: "+ curObj.getKey()+" cannot be deleted because someone has reserved it.");
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
							txMap.get(xid).remove(remoteObj.getKey());
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
						txMap.get(xid).remove(curObj.getKey());
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
							txMap.get(xid).remove(remoteObj.getKey());
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
						txMap.get(xid).remove(curObj.getKey());
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

	public boolean deleteCustomer(int xid, int customerID) throws RemoteException, InvalidTransactionException, TransactionAbortedException
	{
		checkValid(xid);
		cancelTimer(xid);
		
		boolean inRM = false;
		
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
						System.out.println("Customer was found in RM");
						inRM = true;
						writeDataCopy(xid, remoteObj.getKey(), remoteObj);
						txMap.get(xid).add(remoteObj.getKey());
					}
				}

				Customer finalCustomer = (Customer) readDataCopy(xid, "customer-"+customerID);
				RMHashMap reservations = finalCustomer.getReservations();
				for (String reservedKey : reservations.keySet())
				{
					if(lockManager.Lock(xid, reservedKey, TransactionLockObject.LockType.LOCK_WRITE)){
						ReservedItem reserveditem = finalCustomer.getReservedItem(reservedKey);
						Trace.info("RM::deleteCustomer(" + xid + ", " + customerID + ") has reserved " + reserveditem.getKey() + " " +  reserveditem.getCount() +  " times");
						
						ReservableItem curItem = (ReservableItem) readDataCopy(xid, reserveditem.getKey());
						
						if(curItem==null){
							ReservableItem remoteObj=null;
							if(reservedKey.startsWith("flight") ){
								remoteObj = (ReservableItem) Middleware.m_flightsManager.getItem(xid, reserveditem.getKey());
							}
							else if(reservedKey.startsWith("car")){
								remoteObj = (ReservableItem) Middleware.m_carsManager.getItem(xid, reserveditem.getKey());
							}
							else if(reservedKey.startsWith("room")){
								remoteObj = (ReservableItem) Middleware.m_roomsManager.getItem(xid, reserveditem.getKey());
							}

							if(remoteObj == null){
								Trace.warn("Object the client has reserved previously does not exist.");
								resetTimer(xid);
								return false;
							}
							else{
								writeDataCopy(xid, remoteObj.getKey(), remoteObj);
								txMap.get(xid).add(remoteObj.getKey());
							}
						}

						ReservableItem finalItem = (ReservableItem) readDataCopy(xid, reserveditem.getKey());

						finalItem.setReserved(finalItem.getReserved() - reserveditem.getCount());
						finalItem.setCount(finalItem.getCount() + reserveditem.getCount());
						writeDataCopy(xid, finalItem.getKey(), finalItem);
					}
				}
				Trace.info("All reserved items from the customer have been reset.");

				if (inRM) {
					System.out.println("Cust added to delete map");
					toDeleteMap.get(xid).add(finalCustomer.getKey());					
				}
				removeDataCopy(xid, finalCustomer.getKey());
				txMap.get(xid).remove(finalCustomer.getKey());
				
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
						txMap.get(xid).add(remoteObj.getKey());
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
						txMap.get(xid).add(remoteObj.getKey());
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
						txMap.get(xid).add(remoteObj.getKey());
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
						txMap.get(xid).add(remoteCustomer.getKey());
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
						txMap.get(xid).add(remoteItem.getKey());
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
						txMap.get(xid).add(remoteObj.getKey());
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
						txMap.get(xid).add(remoteCustomer.getKey());
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
						txMap.get(xid).add(remoteItem.getKey());
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
						txMap.get(xid).add(remoteCustomer.getKey());
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
						txMap.get(xid).add(remoteItem.getKey());
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
				} catch (InvalidTransactionException e) {
					// TODO Auto-generated catch block
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
		System.out.println(txMap.get(xid));
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