package columnar;

import java.io.*;
import global.*;
import heap.*;
import iterator.FldSpec;


/**
 * Columnarfile Class
 *  
 * 
 * @author aalbaltan
 * 
 */
public class TupleScan implements GlobalConst{
	
	
    /* The columnarfile we are using. */
    private Columnarfile  _cf;  
    
    /* A scan object to read columnar file header */
    private Scan _headerScanner;
    
    /** Constructor
     *
     * @param cf A ColumnarFile object to scan
     * @throws IOException 
     * @throws InvalidTupleSizeException 
     */
	TupleScan(Columnarfile cf) throws 
	InvalidTupleSizeException, 
	IOException
	{
		_cf = cf;
		_headerScanner = _cf._headerFile.openScan();
	}
	
	/** Closes the tuple scan object
    * 
    */
	public void closetuplescan()
	{
		_headerScanner.closescan();
	}
	
	/** Gets the next tuple in columnar file
    *
    * @param out_tid the TID object in which tuple id will be stored
    * @return the next tuple in columnar file  
	 * @throws Exception 
	 * @throws HFBufMgrException 
	 * @throws HFDiskMgrException 
	 * @throws HFException 
	 * @throws InvalidSlotNumberException 
    */
	public Tuple getNext(TID out_tid) throws 
	InvalidSlotNumberException, HFException, HFDiskMgrException, HFBufMgrException, Exception
	{
		// get the next header entry
		RID headerRID = new RID();
		Tuple headerTuple = _headerScanner.getNext(headerRID);
		
		if(headerTuple == null)
			return null;
		
		// convert entry to tuple
		TID tupleID = new TID(headerTuple.getTupleByteArray());
		
		Tuple ret_tuple = _cf.getTuple(tupleID);
		
		out_tid.copyTid(tupleID);		
		return ret_tuple;
	}
	
	public Tuple getNext(TID out_tid, FldSpec[] mask) throws 
	InvalidSlotNumberException, HFException, HFDiskMgrException, HFBufMgrException, Exception
	{
		// get the next header entry
		RID headerRID = new RID();
		Tuple headerTuple = _headerScanner.getNext(headerRID);
		
		if(headerTuple == null)
			return null;
		
		// convert entry to tuple
		TID tupleID = new TID(headerTuple.getTupleByteArray());
		
		Tuple ret_tuple = _cf.getTuple(tupleID, mask);
		
		out_tid.copyTid(tupleID);		
		return ret_tuple;
	}
	
	/** Gets the next tuple in columnar file
    *
    * @param tid the tuple id in which the cursor should be positioned on
    * @return true if cursor was positioned successfully
	 * @throws Exception 
	 * @throws HFBufMgrException 
	 * @throws HFDiskMgrException 
	 * @throws HFException 
	 * @throws InvalidSlotNumberException 
    */
	public boolean position(TID tid) throws 
	InvalidSlotNumberException, 
	HFException, 
	HFDiskMgrException, 
	HFBufMgrException, 
	Exception
	{
		// if a scan already opened, then reset
		closetuplescan();
		
		// reopen
		_headerScanner = _cf._headerFile.openScan();
		
		int position = 0;
		boolean bFound = false;
		TID curr_tid = new TID(_cf._numColumns);
		Tuple t = getNext(curr_tid);
		
		while(!bFound && t != null)
		{
			if(curr_tid.equals(tid))
			{
				bFound = true;
				break;
			}
			++position;
			t = getNext(curr_tid);
		}
		
		if(bFound) // tuple exists
		{
			// if a scan already opened, then reset
			closetuplescan();
			
			// reopen
			_headerScanner = _cf._headerFile.openScan();
			for(int i = 0; i < position; ++i)
				getNext(curr_tid);
		}
		return bFound;
	}

}
