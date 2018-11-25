package Server.Common;

import java.io.*;
public class FileManager
{
	private String fileNamePrefix;
	private BufferedReader br;
	private BufferedWriter bw;
	
	private int filePointer;
	
	private File masterRecord;
	
	public FileManager(String fileNamePrefix)
	{
		this.fileNamePrefix = fileNamePrefix;
		try
		{
			masterRecord = new File(fileNamePrefix+"_master");
			masterRecord.createNewFile();
			File filea = new File(fileNamePrefix+"_A");
			if(filea.length() == 0)
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
		FileInputStream fis = new FileInputStream(fileNamePrefix+"_"+(char) (131 - filePointer));
		ObjectInputStream ois = new ObjectInputStream(fis);
		return (RMHashMap) ois.readObject();
	}
	
	public void writePersistentData(RMHashMap hm) throws Exception
	{
		FileOutputStream fos = new FileOutputStream(fileNamePrefix+"_"+(char) filePointer);
		ObjectOutputStream oos = new ObjectOutputStream(fos);
		oos.writeObject(hm);
		oos.close();
		swap();
	}
	
	private void swap() throws IOException
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
