// -------------------------------
// adapted from Kevin T. Manley
// CSE 593
// -------------------------------

package Server.Common;

import Server.Interface.*;
import Server.Middleware.Middleware;

import java.util.*;
import java.rmi.RemoteException;
import java.io.*;

public class ResourceManager implements IResourceManager
{
	protected String m_name = "";
	protected RMHashMap m_data = new RMHashMap();
	
	// Maps transaction IDs to the keys of objects it has accessed
	private Map<Integer, ArrayList<String>> transactionMap = new HashMap<>();
	// Maps transaction IDs to keys of deleted objects
	private Map<Integer, ArrayList<String>> toDeleteMap = new HashMap<>();
	
	// Maps keys to RMItems
	protected RMHashMap localCopies = new RMHashMap();
	

	public ResourceManager(String p_name)
	{
		m_name = p_name;
	}
	
	// To satisfy the interface, real start method is start(int xid)
	public int start() throws RemoteException 
	{
		return 1;
	}	
	
	// Real start method
	public void start(int xid) {
		transactionMap.put(xid, new ArrayList<String>());
		toDeleteMap.put(xid, new ArrayList<String>());
	}

	
	// Updated - Milestone 3
	/**
	 * TODO: Think about toDelete not removing the items one by one
	 */
	public boolean commit(int xid) throws RemoteException, TransactionAbortedException, InvalidTransactionException{
		
		// Iterate through the items the transaction has created or modified and write them to storage.
		for (String key : transactionMap.get(xid)) 
		{
			RMItem toCommit = readDataCopy(xid, key);
			if (toCommit == null) {
				break;
			}
			else {
				putItem(xid, key, toCommit);
			}
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
				removeData(xid, key);
			}
		}
		
		// Both the transactionMap and the toDeleteMap should be empty at this point -- possible check for debug
		transactionMap.remove(xid);
		toDeleteMap.remove(xid);
		
		Trace.info("Transaction-" + xid + " has committed at the RM");
		return true;
	}
	
	@Override
	// Updated - Milestone 3
	/**
	 * 
	 * TODO Can we really delete all items transaction accessed from the local copy?
	 * If the transaction had created the item - yes, no other transaction depends on it.
	 * If the item has a previous version in storage and transaction modified it, it will have a W lock on it so no other transaction can depend on it.
	 * If the item was only read by the transaction, it's record in the local copy might be shared with other transactions. However, since they were only 
	 * reads, we don't need to remember it's state. In this case, at commit time, RMItem toCommit would be null for the other transactions who have read 
	 * that item and this case is handle in the commit method.
	 */

	public void abort(int xid) throws RemoteException, InvalidTransactionException {

		// Remove all local copy objects associated with this transaction
		for (String key : transactionMap.get(xid)) 
		{
			removeDataCopy(xid, key);
		}
		
		// Remove the transaction's data structures
		transactionMap.remove(xid);
		toDeleteMap.remove(xid);
	}
	
	
	
	
	
	protected RMItem readDataCopy(int xid, String key)
	{
		synchronized(localCopies) {
			RMItem item = localCopies.get(key);
			if (item != null) {
				return (RMItem)item.clone();
			}
			return null;
		}
	}


	protected void writeDataCopy(int xid, String key, RMItem value)
	{
		synchronized(localCopies) {
			localCopies.put(key, value);
		}
	}

	protected void removeDataCopy(int xid, String key)
	{
		synchronized(localCopies) {
			localCopies.remove(key);
		}
	}
	
	
	
	
	
	
	
	
	
	
	public RMItem getItem(int xid, String key) {
		return readData(xid, key);
	}

	
	public void putItem(int xid, String key, RMItem value) {
		Trace.info("Object: " + key +" was added");
		if(value instanceof Flight) Trace.info("Seats: " + ((Flight)value).getCount());
		if(value instanceof Room) Trace.info("Available rooms: " + ((Room)value).getCount());
		if(value instanceof Car) Trace.info("Available cars: " + ((Car)value).getCount());
		
		writeData(xid, key, value);
	}

	public void deleteData(int xid, String key){
		
		Trace.info(key + " deleted");
		removeData(xid, key);
	}


	// Reads a data item
	protected RMItem readData(int xid, String key)
	{
		synchronized(m_data) {
			RMItem item = m_data.get(key);
			if (item != null) {
				return (RMItem)item.clone();
			}
			return null;
		}
	}


	// Writes a data item
	protected void writeData(int xid, String key, RMItem value)
	{
		synchronized(m_data) {
			m_data.put(key, value);
		}
	}

	// Remove the item out of storage
	protected void removeData(int xid, String key)
	{
		synchronized(m_data) {
			m_data.remove(key);
		}
	}

	
	// Updated - Milestone 3
	/**
	 * Case 1: The item is in both the storage and the local copy, that means it was modified by a current transaction but has a previous state in storage. 
	 * In this case, we check if the local copy version (the most recent) has any reservations and if not, 
	 * we remove it from the local copy and add it to the deleteMap so the previous version will be deleted at commit time.
	 * 
	 * Case 2: The item is only in the local copy, that means it was created by this transaction. In this case there is no previous version in storage and 
	 * thus, we check if the local copy version has any reservation. If not we need only remove it from the local copy and not add anything to the 
	 *  delete map (else we will get a null pointer exception at commit time).
	 * 
	 * Case 3: The item is only in storage then we don't need to touch the local copy and simply check if it has any reservations and if not 
	 * add it to the delete map. The stored item will then be deleted at commit time.
	 */
	protected boolean deleteItem(int xid, String key)
	{
		Trace.info("RM::deleteItem(" + xid + ", " + key + ") called");
		
		// Attempt to get the item from storage
		ReservableItem storedObj = (ReservableItem) readData(xid, key);
		
		// Attempt to get item from local copy
		ReservableItem localObj = (ReservableItem) readDataCopy(xid, key);
		
		if (storedObj == null)
		{
			if (localObj == null) 
			{
				Trace.warn("RM::deleteItem(" + xid + ", " + key + ") failed -- item doesn't exist");
				return false;				
			}
			
			else 
			{
				// This is case 2 -- item was created by the transaction; we must only remove the item from the local copy
				if (localObj.getReserved() == 0 ) 
				{
					removeDataCopy(xid, key);
					Trace.info("RM::deleteItem(" + xid + ", " + key + ") item deleted");
					return true;
				}
				else 
				{	
					Trace.info("RM::deleteItem(" + xid + ", " + key + ") item can't be deleted because some customers have reserved it");
					return false;
				}
			}
		}
		else
		{
			if (localObj != null)
			{
				// Case 1 -- item had previous version and was modified by the transaction
				if (localObj.getReserved() == 0)
				{
					toDeleteMap.get(xid).add(key);
					removeDataCopy(xid, localObj.getKey());
					Trace.info("RM::deleteItem(" + xid + ", " + key + ") item marked for deletion");
					return true;
				}
				else
				{
					Trace.info("RM::deleteItem(" + xid + ", " + key + ") item can't be deleted because some customers have reserved it");
					return false;
				}
				
			}
			
			else
			{
				// Case 3 -- item only in storage
				if (storedObj.getReserved() == 0)
				{
					toDeleteMap.get(xid).add(key);
					Trace.info("RM::deleteItem(" + xid + ", " + key + ") item marked for deletion");
					return true;
				}
				else
				{
					Trace.info("RM::deleteItem(" + xid + ", " + key + ") item can't be deleted because some customers have reserved it");
					return false;
				}
			}
		}
	}

	
	
	// Updated - Milestone 3
	// Query the number of available seats/rooms/cars
	protected int queryNum(int xid, String key)
	{
		// Attempt to get the object from localCopy
		ReservableItem curObj = (ReservableItem)readDataCopy(xid, key);
		
		if(curObj==null)
		{
			// If not in local copy, attempt to get it from storage
			ReservableItem storedObj = (ReservableItem) getItem(xid, key);
			if(storedObj!=null)
			{
				// If found, put it in local copy and return count
				writeDataCopy(xid, storedObj.getKey(), storedObj);
				transactionMap.get(xid).add(storedObj.getKey());
				return (storedObj.getCount());
			}
			else
			{
				Trace.info(key + " not found in localCopy or storage");
				return -1;
			}
		}
		else
		{
			return (curObj.getCount());
		}
		
	}    

	// Updated - Milestone 3
	// Query the price of an item
	protected int queryPrice(int xid, String key)
	{
		// Attempt to get the object from localCopy
		ReservableItem curObj = (ReservableItem)readDataCopy(xid, key);
		
		if(curObj==null)
		{
			// If not in local copy, attempt to get it from storage
			ReservableItem storedObj = (ReservableItem) getItem(xid, key);
			if(storedObj!=null)
			{
				// If found, put it in local copy and return count
				writeDataCopy(xid, storedObj.getKey(), storedObj);
				transactionMap.get(xid).add(storedObj.getKey());
				return (storedObj.getPrice());
			}
			else
			{
				Trace.info(key + " not found in localCopy or storage");
				return -1;
			}
		}
		else
		{
			return (curObj.getPrice());
		}      
	}

	// Reserve an item
	protected boolean reserveItem(int xid, int customerID, String key, String location)
	{
		Trace.info("RM::reserveItem(" + xid + ", customer=" + customerID + ", " + key + ", " + location + ") called" );        
		// Read customer object if it exists (and read lock it)
		Customer customer = (Customer)readData(xid, Customer.getKey(customerID));
		if (customer == null)
		{
			Trace.warn("RM::reserveItem(" + xid + ", " + customerID + ", " + key + ", " + location + ")  failed--customer doesn't exist");
			return false;
		} 

		// Check if the item is available
		ReservableItem item = (ReservableItem)readData(xid, key);
		if (item == null)
		{
			Trace.warn("RM::reserveItem(" + xid + ", " + customerID + ", " + key + ", " + location + ") failed--item doesn't exist");
			return false;
		}
		else if (item.getCount() == 0)
		{
			Trace.warn("RM::reserveItem(" + xid + ", " + customerID + ", " + key + ", " + location + ") failed--No more items");
			return false;
		}
		else
		{            
			customer.reserve(key, location, item.getPrice());        
			writeData(xid, customer.getKey(), customer);

			// Decrease the number of available items in the storage
			item.setCount(item.getCount() - 1);
			item.setReserved(item.getReserved() + 1);
			writeData(xid, item.getKey(), item);

			Trace.info("RM::reserveItem(" + xid + ", " + customerID + ", " + key + ", " + location + ") succeeded");
			return true;
		}        
	}

	// Create a new flight, or add seats to existing flight
	// NOTE: if flightPrice <= 0 and the flight already exists, it maintains its current price
	public boolean addFlight(int xid, int flightNum, int flightSeats, int flightPrice) throws RemoteException
	{
		Trace.info("RM::addFlight(" + xid + ", " + flightNum + ", " + flightSeats + ", $" + flightPrice + ") called");
		
		// When adding a new flight, if it was previously marked for deletion, we cancel the deletion 
		if(toDeleteMap.get(xid).contains("flight-"+flightNum))
		{
			toDeleteMap.get(xid).remove("flight-"+flightNum);
			Trace.info("RM::Flight-"+flightNum+" was removed from toDeleteMap" );
		}
		
		// Trying to fetch the flight from our localCopy
		Flight localObj = (Flight)readDataCopy(xid, Flight.getKey(flightNum));
		
		// If not in localCopy
		if(localObj == null)
		{
			Trace.info("Flight-"+flightNum+" not found in local Copy");
			
			// Attempt to get it from storage
			Flight storedObj = (Flight) getItem(xid, "flight-"+flightNum);

			// If we find it in storage
			if(storedObj!=null)
			{
				Trace.info("Flight-"+flightNum+" found in storage");
				storedObj.setCount(storedObj.getCount()+flightSeats);
				if(flightPrice>0) storedObj.setPrice(flightPrice);
				
				// Put the modified flight in local copy to be written to storage at commit time
				writeDataCopy(xid, storedObj.getKey(), storedObj);
				transactionMap.get(xid).add(storedObj.getKey());
				Trace.info("Flight-" + flightNum + " modified from storage, added to localCopy");
			}
			// If not in storage, flight does not exist, so we create it
			else
			{
				Trace.info("Flight-" +flightNum + " not found in storage, creating new flight.");
				Flight newObj = new Flight(flightNum, flightSeats, flightPrice);
				
				//Write it to our local Copy so it will be added to storage at commit time
				writeDataCopy(xid, newObj.getKey(), newObj);
				transactionMap.get(xid).add(newObj.getKey());
				Trace.info("addFlight successful");
			}
		}
		// If already in local Copy, update it there
		else	
		{	
			Trace.info("Flight-" + flightNum + " found in localCopy");
			localObj.setCount(localObj.getCount() + flightSeats);
			if (flightPrice > 0)
			{
				localObj.setPrice(flightPrice);
			}
			writeDataCopy(xid, localObj.getKey(), localObj);
			Trace.info("addFlight successful.");
		}
		return true;	
	}

	// Create a new car location or add cars to an existing location
	// NOTE: if price <= 0 and the location already exists, it maintains its current price
	public boolean addCars(int xid, String location, int count, int price) throws RemoteException
	{
		Trace.info("RM::addCars(" + xid + ", " + location + ", " + count + ", $" + price + ") called");
		Car curObj = (Car)readData(xid, Car.getKey(location));
		if (curObj == null)
		{
			// Car location doesn't exist yet, add it
			Car newObj = new Car(location, count, price);
			writeData(xid, newObj.getKey(), newObj);
			Trace.info("RM::addCars(" + xid + ") created new location " + location + ", count=" + count + ", price=$" + price);
		}
		else
		{
			// Add count to existing car location and update price if greater than zero
			curObj.setCount(curObj.getCount() + count);
			if (price > 0)
			{
				curObj.setPrice(price);
			}
			writeData(xid, curObj.getKey(), curObj);
			Trace.info("RM::addCars(" + xid + ") modified existing location " + location + ", count=" + curObj.getCount() + ", price=$" + price);
		}
		return true;
	}

	// Create a new room location or add rooms to an existing location
	// NOTE: if price <= 0 and the room location already exists, it maintains its current price
	public boolean addRooms(int xid, String location, int count, int price) throws RemoteException
	{
		Trace.info("RM::addRooms(" + xid + ", " + location + ", " + count + ", $" + price + ") called");
		Room curObj = (Room)readData(xid, Room.getKey(location));
		if (curObj == null)
		{
			// Room location doesn't exist yet, add it
			Room newObj = new Room(location, count, price);
			writeData(xid, newObj.getKey(), newObj);
			Trace.info("RM::addRooms(" + xid + ") created new room location " + location + ", count=" + count + ", price=$" + price);
		} else {
			// Add count to existing object and update price if greater than zero
			curObj.setCount(curObj.getCount() + count);
			if (price > 0)
			{
				curObj.setPrice(price);
			}
			writeData(xid, curObj.getKey(), curObj);
			Trace.info("RM::addRooms(" + xid + ") modified existing location " + location + ", count=" + curObj.getCount() + ", price=$" + price);
		}
		return true;
	}

	// Deletes flight
	public boolean deleteFlight(int xid, int flightNum) throws RemoteException
	{
		return deleteItem(xid, Flight.getKey(flightNum));
	}

	// Delete cars at a location
	public boolean deleteCars(int xid, String location) throws RemoteException
	{
		return deleteItem(xid, Car.getKey(location));
	}

	// Delete rooms at a location
	public boolean deleteRooms(int xid, String location) throws RemoteException
	{
		return deleteItem(xid, Room.getKey(location));
	}

	// Returns the number of empty seats in this flight
	public int queryFlight(int xid, int flightNum) throws RemoteException
	{
		return queryNum(xid, Flight.getKey(flightNum));
	}

	// Returns the number of cars available at a location
	public int queryCars(int xid, String location) throws RemoteException
	{
		return queryNum(xid, Car.getKey(location));
	}

	// Returns the amount of rooms available at a location
	public int queryRooms(int xid, String location) throws RemoteException
	{
		return queryNum(xid, Room.getKey(location));
	}

	// Returns price of a seat in this flight
	public int queryFlightPrice(int xid, int flightNum) throws RemoteException
	{
		return queryPrice(xid, Flight.getKey(flightNum));
	}

	// Returns price of cars at this location
	public int queryCarsPrice(int xid, String location) throws RemoteException
	{
		return queryPrice(xid, Car.getKey(location));
	}

	// Returns room price at this location
	public int queryRoomsPrice(int xid, String location) throws RemoteException
	{
		return queryPrice(xid, Room.getKey(location));
	}

	public String queryCustomerInfo(int xid, int customerID) throws RemoteException
	{
		Trace.info("RM::queryCustomerInfo(" + xid + ", " + customerID + ") called");
		Customer customer = (Customer)readData(xid, Customer.getKey(customerID));
		if (customer == null)
		{
			Trace.warn("RM::queryCustomerInfo(" + xid + ", " + customerID + ") failed--customer doesn't exist");
			// NOTE: don't change this--WC counts on this value indicating a customer does not exist...
			return "";
		}
		else
		{
			Trace.info("RM::queryCustomerInfo(" + xid + ", " + customerID + ")");
			System.out.println(customer.getBill());
			return customer.getBill();
		}
	}

	public int newCustomer(int xid) throws RemoteException
	{
        	Trace.info("RM::newCustomer(" + xid + ") called");
		// Generate a globally unique ID for the new customer
		int cid = Integer.parseInt(String.valueOf(xid) +
			String.valueOf(Calendar.getInstance().get(Calendar.MILLISECOND)) +
			String.valueOf(Math.round(Math.random() * 100 + 1)));
		Customer customer = new Customer(cid);
		writeData(xid, customer.getKey(), customer);
		Trace.info("RM::newCustomer(" + cid + ") returns ID=" + cid);
		return cid;
	}

	public boolean newCustomer(int xid, int customerID) throws RemoteException
	{
		Trace.info("RM::newCustomer(" + xid + ", " + customerID + ") called");
		Customer customer = (Customer)readData(xid, Customer.getKey(customerID));
		if (customer == null)
		{
			customer = new Customer(customerID);
			writeData(xid, customer.getKey(), customer);
			Trace.info("RM::newCustomer(" + xid + ", " + customerID + ") created a new customer");
			return true;
		}
		else
		{
			Trace.info("INFO: RM::newCustomer(" + xid + ", " + customerID + ") failed--customer already exists");
			return false;
		}
	}

	public boolean deleteCustomer(int xid, int customerID) throws RemoteException
	{
		Trace.info("RM::deleteCustomer(" + xid + ", " + customerID + ") called");
		Customer customer = (Customer)readData(xid, Customer.getKey(customerID));
		if (customer == null)
		{
			Trace.warn("RM::deleteCustomer(" + xid + ", " + customerID + ") failed--customer doesn't exist");
			return false;
		}
		else
		{            
			// Increase the reserved numbers of all reservable items which the customer reserved. 
 			RMHashMap reservations = customer.getReservations();
			for (String reservedKey : reservations.keySet())
			{        
				ReservedItem reserveditem = customer.getReservedItem(reservedKey);
				Trace.info("RM::deleteCustomer(" + xid + ", " + customerID + ") has reserved " + reserveditem.getKey() + " " +  reserveditem.getCount() +  " times");
				ReservableItem item  = (ReservableItem)readData(xid, reserveditem.getKey());
				Trace.info("RM::deleteCustomer(" + xid + ", " + customerID + ") has reserved " + reserveditem.getKey() + " which is reserved " +  item.getReserved() +  " times and is still available " + item.getCount() + " times");
				item.setReserved(item.getReserved() - reserveditem.getCount());
				item.setCount(item.getCount() + reserveditem.getCount());
				writeData(xid, item.getKey(), item);
			}

			// Remove the customer from the storage
			removeData(xid, customer.getKey());
			Trace.info("RM::deleteCustomer(" + xid + ", " + customerID + ") succeeded");
			return true;
		}
	}

	// Adds flight reservation to this customer
	public boolean reserveFlight(int xid, int customerID, int flightNum) throws RemoteException
	{
		return reserveItem(xid, customerID, Flight.getKey(flightNum), String.valueOf(flightNum));
	}

	// Adds car reservation to this customer
	public boolean reserveCar(int xid, int customerID, String location) throws RemoteException
	{
		return reserveItem(xid, customerID, Car.getKey(location), location);
	}

	// Adds room reservation to this customer
	public boolean reserveRoom(int xid, int customerID, String location) throws RemoteException
	{
		return reserveItem(xid, customerID, Room.getKey(location), location);
	}

	// Reserve bundle 
	public boolean bundle(int xid, int customerId, Vector<String> flightNumbers, String location, boolean car, boolean room) throws RemoteException
	{
		return false;
	}

	public String getName() throws RemoteException
	{
		return m_name;
	}
	
	

	@Override
	public void shutdown() throws RemoteException {
		Trace.info("RM::" + m_name + " ready to shut down");
		System.exit(0);
	}
}
 
