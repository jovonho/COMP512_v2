package Server.Middleware;

import Server.Interface.*;

import java.rmi.RemoteException;
import java.util.Calendar;
import java.util.Vector;

import Server.Common.*;
import Server.LockManager.*;

import java.util.*;
import java.rmi.RemoteException;
import java.io.*;

public class TransactionManager extends Middleware{

	protected int xidCounter = 0;

	private LockManager lockManager = new LockManager();


	private ArrayList<Integer> transactions = new ArrayList<Integer>();
	private ArrayList<Integer> abortedTransactions = new ArrayList<Integer>();

	protected RMHashMap m_dataCopy = new RMHashMap();

	private Map<Integer, ArrayList<String>> txMap = new HashMap();
	private Map<Integer, ArrayList<String>> toDeleteMap = new HashMap();

	public TransactionManager(String name)
	{
		super(name);
	}
	
	

	public int start() throws RemoteException{
		xidCounter++;
		transactions.add(xidCounter);
		txMap.put(xidCounter, new ArrayList<String>());
		toDeleteMap.put(xidCounter, new ArrayList<String>());
		return xidCounter;
	}

	public void abort(int transactionId) throws RemoteException{

	}

	public boolean shutdown() throws RemoteException{
		return false;
	}

	public boolean commit(int transactionId) throws RemoteException, InvalidTransactionException, TransactionAbortedException{
		
		if(abortedTransactions.contains(transactionId)) throw new TransactionAbortedException(transactionId, "The transaction was aborted");
		if(!transactions.contains(transactionId))  throw new InvalidTransactionException(transactionId, "Not a valid transaction ID");


		for(String s : txMap.get(transactionId)){
			RMItem toSend = readDataCopy(transactionId, s);
			Trace.info(s);
			if(toSend == null){
				Trace.info("PROBLEM");
				break;
			}
			if(s.charAt(0) == 'f'){
				m_flightsManager.putItem(transactionId, ((Flight) toSend).getKey(), toSend);
			}
			else if(s.charAt(0) == 'c' && s.charAt(1) == 'a'){
				m_carsManager.putItem(transactionId, ((Car) toSend).getKey(), toSend);
			}
			else if(s.charAt(0) == 'r'){
				m_roomsManager.putItem(transactionId, ((Room) toSend).getKey(), toSend);
			}
			else if(s.charAt(0) == 'c' && s.charAt(1) == 'u'){
				putItem(transactionId, ((Customer)toSend).getKey(), toSend);
			}	
			m_dataCopy.remove(s);
		}

		//need deleteItem
		for(String s : toDeleteMap.get(transactionId)){
			RMItem toDelete = readDataCopy(transactionId, s);
			if(s.charAt(0) == 'f'){
				m_flightsManager.deleteData(transactionId, ((Flight) toDelete).getKey());
			}
			else if(s.charAt(0) == 'c' && s.charAt(1) == 'a'){
				m_carsManager.deleteData(transactionId, ((Car) toDelete).getKey());
			}
			else if(s.charAt(0) == 'r'){
				m_roomsManager.deleteData(transactionId, ((Room) toDelete).getKey());
			}
			else if(s.charAt(0) == 'c' && s.charAt(1) == 'u'){
				deleteData(transactionId, ((Customer)toDelete).getKey());
			}	
			m_dataCopy.remove(s);
		}

		transactions.remove(new Integer(transactionId));
		txMap.remove(transactionId);
		toDeleteMap.remove(transactionId);
		lockManager.UnlockAll(transactionId);
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
		synchronized(m_data) {
			m_dataCopy.remove(key);
		}
	}

	public void deleteDataCopy(int xid, String key){
		removeDataCopy(xid, key);
	}



	public boolean addFlight(int xid, int flightNum, int flightSeats, int flightPrice) throws RemoteException
	{
		try{
			if(toDeleteMap.get(xid).contains("flight-"+flightNum)){
				toDeleteMap.get(xid).remove("flight-"+flightNum);
			}
			if(lockManager.Lock(xid, "flight-"+flightNum, TransactionLockObject.LockType.LOCK_WRITE)){
				Flight curObj = (Flight)readDataCopy(xid, Flight.getKey(flightNum));
				if(curObj == null){
					Flight remoteObj = (Flight) m_flightsManager.getItem(xid, "flight-"+flightNum);
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
		return true;
	}

	public boolean addCars(int xid, String location, int count, int price) throws RemoteException
	{
		try{
			if(toDeleteMap.get(xid).contains("car-"+location)){
				toDeleteMap.get(xid).remove("car-"+location);
			}

			if(lockManager.Lock(xid, "car-"+location, TransactionLockObject.LockType.LOCK_WRITE)){
				Car curObj = (Car) readDataCopy(xid, Car.getKey(location));
				if(curObj == null){
					Car remoteObj = (Car) m_carsManager.getItem(xid, "car-"+location);
					if(remoteObj !=null){
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
						Trace.info("Local copy added and write addCars successful.");
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
		return true;	
	}

	public boolean addRooms(int xid, String location, int count, int price) throws RemoteException
	{
		try{
			if(toDeleteMap.get(xid).contains("room-"+location)){
				toDeleteMap.get(xid).remove("room-"+location);
			}

			if(lockManager.Lock(xid, "room-"+location, TransactionLockObject.LockType.LOCK_WRITE)){
				Room curObj = (Room) readDataCopy(xid, Room.getKey(location));
				if(curObj==null){
					Room remoteObj = (Room) m_roomsManager.getItem(xid, "room-"+location);
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
		return true;
	}

	public int newCustomer(int xid) throws RemoteException
	{
		try{
			int cid = Integer.parseInt(String.valueOf(xid) + String.valueOf(Calendar.getInstance().get(Calendar.MILLISECOND)) + String.valueOf(Math.round(Math.random() * 100 + 1)));
			if(toDeleteMap.get(xid).contains("customer-"+cid)){
				toDeleteMap.get(xid).remove("customer-"+cid);
			}
			if(lockManager.Lock(xid, "customer-"+cid, TransactionLockObject.LockType.LOCK_WRITE)){
				Customer customer = new Customer(cid);
				writeDataCopy(xid, customer.getKey(), customer);
				txMap.get(xid).add(customer.getKey());
				Trace.info("Customer: " + cid + "has been written locally.");
				return cid;
			}
		}catch(DeadlockException e){
			abort(xid);
			abortedTransactions.add(xid);
			return 0;
		}
		return 0;
	}

	public boolean newCustomer(int xid, int customerId) throws RemoteException
	{
		try{
			if(lockManager.Lock(xid, "customer-"+customerId, TransactionLockObject.LockType.LOCK_WRITE)){
				Customer customer = (Customer) readDataCopy(xid, "customer-"+ customerId);

				if(customer==null){
					Customer remoteCustomer = (Customer) readData(xid, "customer-"+customerId);
					if(remoteCustomer==null){
						Customer newCustomer = new Customer(customerId);
						writeDataCopy(xid, newCustomer.getKey(), newCustomer);
						txMap.get(xid).add(newCustomer.getKey());
						Trace.info("New Customer: "+customerId+"was sucessfully created i Local copy.");
						return true;
					}
					else{
						Trace.info("Customer already exists in remote. Failed adding customer.");
						return false;
					}
				}
				else{
					Trace.info("Customer already exists in Local Copy. Failed adding customer.");
					return false;
				}
			}
		}catch(DeadlockException e){
			abort(xid);
			abortedTransactions.add(xid);
			return false;
		}
		return true;
	}

	public int queryFlight(int xid, int flightNum) throws RemoteException
	{
		try{
			if(lockManager.Lock(xid, "flight-"+flightNum, TransactionLockObject.LockType.LOCK_READ)){
				Flight curObj = (Flight)readDataCopy(xid, "flight-"+flightNum);
				if(curObj==null){
					Flight remoteObj = (Flight) m_flightsManager.getItem(xid, "flight-"+flightNum);
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

	public boolean reserveFlight(int xid, int customerID, int flightNum) throws RemoteException{

		try{
			if(lockManager.Lock(xid, "customer-"+customerID, TransactionLockObject.LockType.LOCK_WRITE) && lockManager.Lock(xid, "flight-"+flightNum, TransactionLockObject.LockType.LOCK_WRITE)){
				Customer curCustomer = (Customer) readDataCopy(xid, "customer-"+customerID);
				if(curCustomer == null){
					Customer remoteCustomer = (Customer) readData(xid, "customer-"+customerID);
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
					Flight remoteItem = (Flight) m_flightsManager.getItem(xid, "flight-"+flightNum);
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

	public boolean deleteFlight(int xid, int flightNum) throws RemoteException
	{
		try{
			if(lockManager.Lock(xid, "flight-"+flightNum, TransactionLockObject.LockType.LOCK_WRITE)){
				Flight curObj = (Flight) readDataCopy(xid, "flight-"+flightNum);
				if(curObj == null){
					Flight remoteObj = (Flight) m_flightsManager.getItem(xid, "flight-"+flightNum);
					if(remoteObj == null){
						Trace.warn("The item does not exist and thus cannot be deleted.");
						return false;
					}
					else{
						toDeleteMap.get(xid).add(remoteObj.getKey());
						Trace.info("Flight: " + remoteObj.getKey()+" is added to delete map.");
					}
				}
				else{
					removeDataCopy(xid, curObj.getKey());
					toDeleteMap.get(xid).add(curObj.getKey());
				}
			}


		}catch(DeadlockException e){
			abort(xid);
			abortedTransactions.add(xid);
			return false;
		}
	}

}