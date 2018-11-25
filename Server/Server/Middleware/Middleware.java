package Server.Middleware;

import Server.Interface.*;

import java.rmi.RemoteException;
import java.util.Calendar;
import java.util.Vector;

import Server.Interface.InvalidTransactionException;
import Server.Interface.TransactionAbortedException;
import Server.LockManager.DeadlockException;
import Server.Common.*;


public abstract class Middleware implements IResourceManager {

	protected String m_name = "";
	protected RMHashMap m_data = new RMHashMap();
	
	static IResourceManager m_flightsManager = null;
	static IResourceManager m_carsManager = null;
	static IResourceManager m_roomsManager = null;
	
	static TransactionManager txManager = null;
	
	protected FileManager m_fileManager;


	public Middleware(String p_name)
	{   
		m_name = p_name;
		txManager = new TransactionManager(this);
		m_fileManager = new FileManager(p_name);
		
		try {
			m_data = m_fileManager.getPersistentData();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	public int start() throws RemoteException {
		return txManager.start();
	}
	
	public void start(int xid) throws RemoteException{
	}
	
	public void abort(int xid) throws InvalidTransactionException, RemoteException {
			txManager.abort(xid);
	}
	
	public void shutdown() throws RemoteException {
		try {
			shutdownFlights();
		} catch (Exception e) {
			try { shutdownCars();
			}
			catch (Exception e2) {
				shutdownRooms();
			}
		} finally {
			System.exit(0);
		}
	}
	
	public boolean commit(int transactionId) throws RemoteException, InvalidTransactionException, TransactionAbortedException {
		return txManager.commit(transactionId);
	}
	
	public void storeMapPersistent(){
		try {
			m_fileManager.writePersistentData(m_data);
		} catch (Exception e) {
			e.printStackTrace();
		}
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

	public RMItem getItem(int xid, String key) {
		return readData(xid, key);
	}

	public void deleteData(int xid, String key){
		removeData(xid, key);
	}

	// Writes a data item
	protected void writeData(int xid, String key, RMItem value)
	{
		synchronized(m_data) {
			m_data.put(key, value);
		}
	}
	
	public void putItem(int xid, String key, RMItem value) {
		writeData(xid, key, value);
		Trace.info("Customer: "+key+"has been added to the remote server.");
	}
	
	
	// Remove the item out of storage
	protected void removeData(int xid, String key)
	{
		synchronized(m_data) {
			m_data.remove(key);
		}
	}

	// Deletes the encar item
	protected boolean deleteItem(int xid, String key)
	{
		Trace.info("RM::deleteItem(" + xid + ", " + key + ") called");
		ReservableItem curObj = (ReservableItem)readData(xid, key);
		// Check if there is such an item in the storage
		if (curObj == null)
		{
			Trace.warn("RM::deleteItem(" + xid + ", " + key + ") failed--item doesn't exist");
			return false;
		}
		else
		{
			if (curObj.getReserved() == 0)
			{
				removeData(xid, curObj.getKey());
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

	// Query the number of available seats/rooms/cars
	protected int queryNum(int xid, String key)
	{
		Trace.info("RM::queryNum(" + xid + ", " + key + ") called");
		ReservableItem curObj = (ReservableItem)readData(xid, key);
		int value = 0;  
		if (curObj != null)
		{
			value = curObj.getCount();
		}
		Trace.info("RM::queryNum(" + xid + ", " + key + ") returns count=" + value);
		return value;
	}    

	// Query the price of an item
	protected int queryPrice(int xid, String key)
	{
		Trace.info("RM::queryPrice(" + xid + ", " + key + ") called");
		ReservableItem curObj = (ReservableItem)readData(xid, key);
		int value = 0; 
		if (curObj != null)
		{
			value = curObj.getPrice();
		}
		Trace.info("RM::queryPrice(" + xid + ", " + key + ") returns cost=$" + value);
		return value;        
	}

	// Reserve an item
	protected boolean reserveItem(int xid, int customerID, String key, String location) throws RemoteException
	{
		Trace.info("RM::reserveItem(" + xid + ", customer=" + customerID + ", " + key + ", " + location + ") called" );  
		
		// Read customer object if it exists (and read lock it)
		Customer customer = (Customer)readData(xid, Customer.getKey(customerID));
		if (customer == null)
		{
			Trace.warn("RM::reserveItem(" + xid + ", " + customerID + ", " + key + ", " + location + ")  failed--customer doesn't exist");
			return false;
		} 
		
		if (key.startsWith("flight")) 
		{
			ReservableItem item = (ReservableItem) m_flightsManager.getItem(xid, key);
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
				m_flightsManager.putItem(xid, item.getKey(), item);

				Trace.info("RM::reserveItem(" + xid + ", " + customerID + ", " + key + ", " + location + ") succeeded");
				return true;
			}
		}
		else if (key.startsWith("car"))
		{
			ReservableItem item = (ReservableItem) m_carsManager.getItem(xid, key);
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
				m_carsManager.putItem(xid, item.getKey(), item);

				Trace.info("RM::reserveItem(" + xid + ", " + customerID + ", " + key + ", " + location + ") succeeded");
				return true;
			}
		}
		else
		{
			ReservableItem item = (ReservableItem) m_roomsManager.getItem(xid, key);
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
				m_roomsManager.putItem(xid, item.getKey(), item);

				Trace.info("RM::reserveItem(" + xid + ", " + customerID + ", " + key + ", " + location + ") succeeded");
				return true;
			}
		}      
	}



	// Create a new flight, or add seats to existing flight
	// NOTE: if flightPrice <= 0 and the flight already exists, it maintains its current price
	public boolean addFlight(int xid, int flightNum, int flightSeats, int flightPrice) throws RemoteException, InvalidTransactionException, TransactionAbortedException
	{
		Trace.info("Middleware::addFlight(" + xid + ", " + flightNum + ", " + flightSeats + ", $" + flightPrice + ") called");
		return txManager.addFlight(xid, flightNum, flightSeats, flightPrice);
	}

	// Create a new car location or add cars to an existing location
	// NOTE: if price <= 0 and the location already exists, it maintains its current price
	public boolean addCars(int xid, String location, int count, int price) throws RemoteException, InvalidTransactionException, TransactionAbortedException
	{
		Trace.info("RM::addCars(" + xid + ", " + location + ", " + count + ", $" + price + ") called");
		return txManager.addCars(xid, location, count, price);
	}

	// Create a new room location or add rooms to an existing location
	// NOTE: if price <= 0 and the room location already exists, it maintains its current price
	public boolean addRooms(int xid, String location, int count, int price) throws RemoteException, InvalidTransactionException, TransactionAbortedException
	{
		Trace.info("RM::addRooms(" + xid + ", " + location + ", " + count + ", $" + price + ") called");
		return txManager.addRooms(xid, location, count, price);
	}

	// Deletes flight
	public boolean deleteFlight(int xid, int flightNum) throws RemoteException, InvalidTransactionException, TransactionAbortedException
	{
		return txManager.deleteFlight(xid, flightNum);
	}

	// Delete cars at a location
	public boolean deleteCars(int xid, String location) throws RemoteException, InvalidTransactionException, TransactionAbortedException
	{
		return txManager.deleteCars(xid, location);
	}

	// Delete rooms at a location
	public boolean deleteRooms(int xid, String location) throws RemoteException, InvalidTransactionException, TransactionAbortedException
	{
		return txManager.deleteRooms(xid, location);
	}

	// Returns the number of empty seats in this flight
	public int queryFlight(int xid, int flightNum) throws RemoteException, InvalidTransactionException, TransactionAbortedException
	{
		return txManager.queryFlight(xid, flightNum);
	}

	// Returns the number of cars available at a location
	public int queryCars(int xid, String location) throws RemoteException, InvalidTransactionException, TransactionAbortedException
	{
		return txManager.queryCars(xid, location);
	}

	// Returns the amount of rooms available at a location
	public int queryRooms(int xid, String location) throws RemoteException, InvalidTransactionException, TransactionAbortedException
	{
		return txManager.queryRooms(xid, location);
	}

	// Returns price of a seat in this flight
	public int queryFlightPrice(int xid, int flightNum) throws RemoteException, InvalidTransactionException, TransactionAbortedException
	{
		return txManager.queryFlightPrice(xid, flightNum);
	}

	// Returns price of cars at this location
	public int queryCarsPrice(int xid, String location) throws RemoteException, InvalidTransactionException, TransactionAbortedException
	{
		return txManager.queryCarsPrice(xid, location);
	}

	// Returns room price at this location
	public int queryRoomsPrice(int xid, String location) throws RemoteException, InvalidTransactionException, TransactionAbortedException
	{
		return txManager.queryRoomsPrice(xid, location);
	}

	public String queryCustomerInfo(int xid, int customerID) throws RemoteException, InvalidTransactionException, TransactionAbortedException
	{
		return txManager.queryCustomerInfo(xid, customerID);
	}

	public int newCustomer(int xid) throws RemoteException, InvalidTransactionException, TransactionAbortedException
	{
		return txManager.newCustomer(xid);
	}

	public boolean newCustomer(int xid, int customerID) throws RemoteException, InvalidTransactionException, TransactionAbortedException
	{
		return txManager.newCustomer(xid, customerID);
	}

	public boolean deleteCustomer(int xid, int customerID) throws RemoteException, InvalidTransactionException, TransactionAbortedException
	{
		return txManager.deleteCustomer(xid, customerID);
	}

	// Adds flight reservation to this customer
	public boolean reserveFlight(int xid, int customerID, int flightNum) throws RemoteException, InvalidTransactionException, TransactionAbortedException
	{
		return txManager.reserveFlight(xid, customerID, flightNum);
	}

	// Adds car reservation to this customer
	public boolean reserveCar(int xid, int customerID, String location) throws RemoteException, InvalidTransactionException, TransactionAbortedException
	{
		return txManager.reserveCar(xid, customerID, location);
	}

	// Adds room reservation to this customer
	public boolean reserveRoom(int xid, int customerID, String location) throws RemoteException, InvalidTransactionException, TransactionAbortedException
	{
		return txManager.reserveRoom(xid, customerID, location);
	}

	// Reserve bundle 
	public boolean bundle(int xid, int customerID, Vector<String> flightNumbers, String location, boolean car, boolean room) throws RemoteException, NumberFormatException, InvalidTransactionException, TransactionAbortedException
	{
		return txManager.bundle(xid, customerID, flightNumbers, location, car, room);
	}

	public String getName() throws RemoteException
	{
		return m_name;
	}
	public void shutdownFlights() throws RemoteException {
		m_flightsManager.shutdown();
	}
	
	public void shutdownCars() throws RemoteException {
		m_carsManager.shutdown();		
	}
	
	public void shutdownRooms() throws RemoteException {
		m_roomsManager.shutdown();		
	}

	//need this to satisfy interface
	public boolean changeObject(int xid, String key, int count) throws RemoteException {
		return false;
	}
}
