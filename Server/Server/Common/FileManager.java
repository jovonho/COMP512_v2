package Server.Common;

import java.io.*;
public class FileManager
{
	/**
	 * Name of resource type we are dealing with.
	 */
	private String fileNamePrefix;
	private BufferedReader br;
	private BufferedWriter bw;
	
	/**
	 * Pointer on RAM to active disk record.
	 */
	private int filePointer;
	/**
	 * Pointer on disk to active disk record.
	 */
	private File masterRecord;
	
	public FileManager(String namePrefix)
	{
		this.fileNamePrefix = "records//" + namePrefix;								// Set the filename. This includes a "records" directory
		try
		{
			masterRecord = new File(fileNamePrefix+"_master");						// Make a file object.
			masterRecord.createNewFile();											// Create the file on disk if it does not exist.
			File filea = new File(fileNamePrefix+"_A");

			if(filea.length() == 0)													// If file A is empty, then we need to populate it with an empty RMHashmap.
			{
				RMHashMap emptymap = new RMHashMap();
				FileOutputStream fos = new FileOutputStream(fileNamePrefix+"_A");
				ObjectOutputStream oos = new ObjectOutputStream(fos);
				oos.writeObject(emptymap);
				oos.close();
			}
			File fileb = new File(fileNamePrefix+"_B");
			if(fileb.length() == 0)
			{
				RMHashMap emptymap = new RMHashMap();
				FileOutputStream fos = new FileOutputStream(fileNamePrefix+"_B");
				ObjectOutputStream oos = new ObjectOutputStream(fos);
				oos.writeObject(emptymap);
				oos.close();
			}

			if (masterRecord.length() == 0)
			{
			    System.out.println("file empty");
			    bw = new BufferedWriter(new FileWriter(masterRecord));
			    bw.write("A");
			    bw.close();
			}

			br = new BufferedReader(new FileReader(masterRecord));
			filePointer = br.read();
			br.close();
		}
		catch (IOException e)
		{
			e.printStackTrace();
		}
	}
	
	public RMHashMap getPersistentData() throws Exception
	{
		System.out.println(fileNamePrefix);
		FileInputStream fis = new FileInputStream(fileNamePrefix+"_"+(char) (131 - filePointer));
		ObjectInputStream ois = new ObjectInputStream(fis);
		RMHashMap ret = (RMHashMap) ois.readObject();
		ois.close();
		fis.close();
		return ret;
	}
	
	public void writePersistentData(RMHashMap hm) throws Exception
	{
		FileOutputStream fos = new FileOutputStream(fileNamePrefix+"_"+(char) filePointer);
		ObjectOutputStream oos = new ObjectOutputStream(fos);
		oos.writeObject(hm);
		oos.close();
		fos.close();
		swap();
	}
	
	public void writePersistentNoSwap(RMHashMap hm)
	{
		try {
			FileOutputStream fos = new FileOutputStream(fileNamePrefix+"_"+(char) filePointer);
			ObjectOutputStream oos = new ObjectOutputStream(fos);
			oos.writeObject(hm);
			oos.close();
			fos.close();			
		}
		catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	public void swap() throws IOException
	{
		br = new BufferedReader(new FileReader(masterRecord));
		bw = new BufferedWriter(new FileWriter(masterRecord));
		
		if(filePointer == 65)
			filePointer = 66;
		else
			filePointer = 65;
		bw.write(filePointer);
			
		bw.close();
		br.close();
		System.out.println("Master file pointer successfully swapped");
	}
}
