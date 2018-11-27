package Server.Interface;

import java.rmi.Remote;
import java.rmi.RemoteException;

import Server.Common.*;
import Server.LockManager.DeadlockException;

import java.util.*;

/** 
 * Simplified version from CSE 593 Univ. of Washington
 *
 * Distributed System in Java.
 * 
 * failure reporting is done using two pieces, exceptions and boolean 
 * return values.  Exceptions are used for systemy things. Return
 * values are used for operations that would affect the consistency
 * 
 * If there is a boolean return value and you're not sure how it 
 * would be used in your implementation, ignore it.  I used boolean
 * return values in the interface generously to allow flexibility in 
 * implementation.  But don't forget to return true when the operation
 * has succeeded.
 */

public interface IResourceManager extends Remote 
{
	


	Object m_data = null;
	FileManager m_fileManager = null;

	public RMItem getItem(int xid, String key)
	throws RemoteException;
	
	public void putItem(int xid, String key, RMItem value)
	throws RemoteException;

    public void deleteData(int xid, String key)
    throws RemoteException;

    public int start()
    throws RemoteException;

    public boolean commit(int transactionId)
    throws RemoteException, InvalidTransactionException, TransactionAbortedException;
	
    public void abort(int xid) throws RemoteException, InvalidTransactionException;
    
    public void shutdown() throws RemoteException;

    
    public boolean changeObject(int xid, String key, int count) throws RemoteException;
	
    //functions for crash API
    public void resetCrashes() throws RemoteException;
    public void crashMiddleware(int mode) throws RemoteException;
    public void crashResourceManager(String name, int mode) throws RemoteException;
    
    /**
     * Add seats to a flight.
     *
     * In general this will be used to create a new
     * flight, but it should be possible to add seats to an existing flight.
     * Adding to an existing flight should overwrite the current price of the
     * available seats.
     *
     * @return Success
     * @throws TransactionAbortedException 
     * @throws InvalidTransactionException 
     */
    public boolean addFlight(int id, int flightNum, int flightSeats, int flightPrice) 
	throws RemoteException, InvalidTransactionException, TransactionAbortedException; 
    
    /**
     * Add car at a location.
     *
     * This should look a lot like addFlight, only keyed on a string location
     * instead of a flight number.
     *
     * @return Success
     * @throws TransactionAbortedException 
     * @throws InvalidTransactionException 
     */
    public boolean addCars(int id, String location, int numCars, int price) 
	throws RemoteException, InvalidTransactionException, TransactionAbortedException; 
   
    /**
     * Add room at a location.
     *
     * This should look a lot like addFlight, only keyed on a string location
     * instead of a flight number.
     *
     * @return Success
     * @throws TransactionAbortedException 
     * @throws InvalidTransactionException 
     */
    public boolean addRooms(int id, String location, int numRooms, int price) 
	throws RemoteException, InvalidTransactionException, TransactionAbortedException; 			    
			    
    /**
     * Add customer.
     *
     * @return Unique customer identifier
     * @throws TransactionAbortedException 
     * @throws InvalidTransactionException 
     */
    public int newCustomer(int id) 
	throws RemoteException, InvalidTransactionException, TransactionAbortedException; 
    
    /**
     * Add customer with id.
     *
     * @return Success
     * @throws TransactionAbortedException 
     * @throws InvalidTransactionException 
     */
    public boolean newCustomer(int id, int cid)
        throws RemoteException, InvalidTransactionException, TransactionAbortedException;

    /**
     * Delete the flight.
     *
     * deleteFlight implies whole deletion of the flight. If there is a
     * reservation on the flight, then the flight cannot be deleted
     *
     * @return Success
     * @throws TransactionAbortedException 
     * @throws InvalidTransactionException 
     */   
    public boolean deleteFlight(int id, int flightNum) 
	throws RemoteException, InvalidTransactionException, TransactionAbortedException; 
    
    /**
     * Delete all cars at a location.
     *
     * It may not succeed if there are reservations for this location
     *
     * @return Success
     * @throws TransactionAbortedException 
     * @throws InvalidTransactionException 
     */		    
    public boolean deleteCars(int id, String location) 
	throws RemoteException, InvalidTransactionException, TransactionAbortedException; 

    /**
     * Delete all rooms at a location.
     *
     * It may not succeed if there are reservations for this location.
     *
     * @return Success
     * @throws TransactionAbortedException 
     * @throws InvalidTransactionException 
     */
    public boolean deleteRooms(int id, String location) 
	throws RemoteException, InvalidTransactionException, TransactionAbortedException; 
    
    /**
     * Delete a customer and associated reservations.
     *
     * @return Success
     * @throws TransactionAbortedException 
     * @throws InvalidTransactionException 
     */
    public boolean deleteCustomer(int id, int customerID) 
	throws RemoteException, InvalidTransactionException, TransactionAbortedException; 

    /**
     * Query the status of a flight.
     *
     * @return Number of empty seats
     * @throws TransactionAbortedException 
     * @throws InvalidTransactionException 
     */
    public int queryFlight(int id, int flightNumber) 
	throws RemoteException, InvalidTransactionException, TransactionAbortedException; 

    /**
     * Query the status of a car location.
     *
     * @return Number of available cars at this location
     * @throws TransactionAbortedException 
     * @throws InvalidTransactionException 
     */
    public int queryCars(int id, String location) 
	throws RemoteException, InvalidTransactionException, TransactionAbortedException; 

    /**
     * Query the status of a room location.
     *
     * @return Number of available rooms at this location
     * @throws TransactionAbortedException 
     * @throws InvalidTransactionException 
     */
    public int queryRooms(int id, String location) 
	throws RemoteException, InvalidTransactionException, TransactionAbortedException; 

    /**
     * Query the customer reservations.
     *
     * @return A formatted bill for the customer
     * @throws TransactionAbortedException 
     * @throws InvalidTransactionException 
     */
    public String queryCustomerInfo(int id, int customerID) 
	throws RemoteException, InvalidTransactionException, TransactionAbortedException; 
    
    /**
     * Query the status of a flight.
     *
     * @return Price of a seat in this flight
     * @throws TransactionAbortedException 
     * @throws InvalidTransactionException 
     */
    public int queryFlightPrice(int id, int flightNumber) 
	throws RemoteException, InvalidTransactionException, TransactionAbortedException; 

    /**
     * Query the status of a car location.
     *
     * @return Price of car
     * @throws TransactionAbortedException 
     * @throws InvalidTransactionException 
     */
    public int queryCarsPrice(int id, String location) 
	throws RemoteException, InvalidTransactionException, TransactionAbortedException; 

    /**
     * Query the status of a room location.
     *
     * @return Price of a room
     * @throws TransactionAbortedException 
     * @throws InvalidTransactionException 
     */
    public int queryRoomsPrice(int id, String location) 
	throws RemoteException, InvalidTransactionException, TransactionAbortedException; 

    /**
     * Reserve a seat on this flight.
     *
     * @return Success
     * @throws TransactionAbortedException 
     * @throws InvalidTransactionException 
     */
    public boolean reserveFlight(int id, int customerID, int flightNumber) 
	throws RemoteException, InvalidTransactionException, TransactionAbortedException; 

    /**
     * Reserve a car at this location.
     *
     * @return Success
     * @throws TransactionAbortedException 
     * @throws InvalidTransactionException 
     */
    public boolean reserveCar(int id, int customerID, String location) 
	throws RemoteException, InvalidTransactionException, TransactionAbortedException; 

    /**
     * Reserve a room at this location.
     *
     * @return Success
     * @throws TransactionAbortedException 
     * @throws InvalidTransactionException 
     */
    public boolean reserveRoom(int id, int customerID, String location) 
	throws RemoteException, InvalidTransactionException, TransactionAbortedException; 

    /**
     * Reserve a bundle for the trip.
     *
     * @return Success
     * @throws TransactionAbortedException 
     * @throws InvalidTransactionException 
     * @throws NumberFormatException 
     * @throws DeadlockException 
     */
    public boolean bundle(int id, int customerID, Vector<String> flightNumbers, String location, boolean car, boolean room)
	throws RemoteException, NumberFormatException, InvalidTransactionException, TransactionAbortedException; 

    /**
     * Convenience for probing the resource manager.
     *
     * @return Name
     */
    public String getName()
        throws RemoteException;

	public void start(int xid) throws RemoteException;

	public void storeMapPersistent() throws RemoteException;

	public void resetTimer() throws RemoteException;

	public void cancelTimer() throws RemoteException;

	public boolean prepare(int xid) throws RemoteException;

//	public Object getMapClone() throws RemoteException;
//
//	public void storeMapPersistentNoSwap(RMHashMap temp) throws RemoteException;
	
	public void fileManagerSwap() throws RemoteException;
	
	public void updateStorage() throws RemoteException;
}
