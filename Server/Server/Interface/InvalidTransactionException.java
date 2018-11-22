package Server.Interface;

public class InvalidTransactionException extends Exception
{
	private int m_xid = 0;

	public InvalidTransactionException(int xid, String msg)
	{
		super("The transaction " + xid + " is not a valid transaction ID: " + msg);
		m_xid = xid;
	}

	int getXId()
	{
		return m_xid;
	}
}