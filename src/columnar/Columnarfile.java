package columnar;

import java.io.IOException;

import global.*;
import heap.*;

// TODO(aalbaltan) to be replaced later
class TupleScan{
	
}

interface  Filetype {
	  int TEMP = 0;
	  int ORDINARY = 1;
	  
	} // end of Filetype

/**
 * Columnarfile Class
 *  
 * 
 * @author aalbaltan
 * 
 */

public class Columnarfile implements Filetype,  GlobalConst {
	
	// Phase 2 document defines numColumns as static int. This means all columnar
	// files created are required to have the same number of columns. 
	
	/* Number of columns/attributes of this relation */
	static int _numColumns;
	
	/* Type of each column/attribute */
	AttrType[] _type;
	
	short[] _stringsSizes;
	
	/* Columanrfile header file. The entries in the header are of type TID. */ 
	Heapfile _headerFile;
	
	/* A heapfile for each column in the columnarfile */
	Heapfile[] _columnsFiles;
	
	int _ftype;
	
	/* A flag to track file status */ 
	private boolean _file_deleted;
	
	/* Columnarfile name*/
	private String _fileName;
	
	/**
	 * Constructor
	 * if columnar file does not exist, create one heapfile ("name.columnid") per column;
	 * 		also create a "name.hdr" file that contains relevant metadata. 
	 * 
	 * @param name
	 *            name of the file to create/open
	 * @param numColumns
	 *            number of columns in the file
	 * @param type
	 * 			  type of the columns
	 * 
	 * @exception java.lang.IllegalArgumentException
	 */
	public Columnarfile(String name, int numColumns, AttrType[] type, short[] stringsSizes) throws 
		IllegalArgumentException,
		HFException,
		HFBufMgrException,
		HFDiskMgrException,
		IOException
	{
		_file_deleted = true;
		
		if(numColumns != type.length)
			throw new IllegalArgumentException("number of columns does not match attributes types");
		
		if(name == null)
			throw new IllegalArgumentException("invalid file name");
		
		_fileName = name;
		_ftype = ORDINARY;
		
		// TODO(aalbaltan) it's assumed here that associated files (header file + columns files) are in 
		// valid state, i.e. either they all exist or none exist. A proper check could be added here to 
		// make sure all files are consistent.
		
		_numColumns = numColumns;
		_columnsFiles = new Heapfile[numColumns];
		_type = new AttrType[numColumns];
		_type = type;
		
		_stringsSizes = new short[stringsSizes.length];
		_stringsSizes = stringsSizes;
		
		// Open or create files
		_headerFile = new Heapfile(_fileName + ".hdr");
		for(int i = 0; i < numColumns; ++i)
			_columnsFiles[i] = new Heapfile(_fileName + "." + Integer.toString(i));
		
		_file_deleted = false;
	}
	
	/**
	 * Delete all relevant files from the database. The number of files that will be deleted is equal
	 * to the number of columns of this file.
	 * 
	 */
	public void deleteColumnarFile() throws 
		InvalidSlotNumberException, 
		FileAlreadyDeletedException,
		InvalidTupleSizeException, 
		HFBufMgrException, 
		HFDiskMgrException, 
		IOException {
		if (_file_deleted)
			throw new FileAlreadyDeletedException(null, "file already deleted");

		// Mark the deleted flag (even if it doesn't get all the way done)
		_file_deleted = true;

		// delete all associated heap files
		_headerFile.deleteFile();
		for(int i = 0; i < _numColumns; ++i)
			_columnsFiles[i].deleteFile();
	}
	
	
	/**
	 * Inserts a new tuple into the file
	 * 
	 * NOTE#2(aalbaltan): I finally was able to figure out how tuples should be used. The method described in NOTE#1 is not 
	 * used anymore as it caused some serious problems with other issues. I decided to keep NOTE#1 as reference. 
	 * 
	 * NOTE#1(aalbaltan): there wasn't much explanation on the format of tuplePtr. There are different ways to do
	 * the format. My first thought was to use the Tuple class. However, the class was not documented properly 
	 * and it took me a while to figure out proper usage. As a result, I decided to a format similar to the 
	 * following example.
	 * 
	 * 	Example: Table header: int	String	float
	 * 	Encoding Format: int (4 bytes) short (2 bytes) String (n bytes) + float (4 bytes)
	 * 
	 *   Basically, the size of a string is stored right before it.
	 *   
	 * Unfortunately, the decision to use this format has complicated matters. Due to time restrictions, it's too
	 * now to fix the current format. Perhaps in phase 3 a better format could be used instead.
	 * 
	 * @param tuplePtr
	 *            the tuple to be inserted
	 *            
	 * @return TID of the newly inserted tuple
	 * 
	 * @throws java.lang.IllegalArgumentException
	 * @throws IOException 
	 * @throws FieldNumberOutOfBoundException 
	 * @throws HFDiskMgrException 
	 * @throws HFBufMgrException 
	 * @throws HFException 
	 * @throws SpaceNotAvailableException 
	 * @throws InvalidTupleSizeException 
	 * @throws InvalidSlotNumberException 
	 * @throws InvalidTypeException 
	 */
	public TID insertTuple(byte[] tuplePtr) throws
		IllegalArgumentException, 
		FieldNumberOutOfBoundException, 
		IOException, 
		InvalidSlotNumberException, 
		InvalidTupleSizeException, 
		SpaceNotAvailableException, 
		HFException, 
		HFBufMgrException, 
		HFDiskMgrException, 
		InvalidTypeException
	{
		// sanity checks
		if(tuplePtr.length == 0)
			throw new IllegalArgumentException("cannot insert empty tuple");
		
		if(tuplePtr.length > Tuple.max_size)
			throw new IllegalArgumentException("tuple size too big");
		
		RID[] recordsID = new RID[_numColumns];
		byte[] data = null;
		
		Tuple tuple = new Tuple(tuplePtr, 0, Tuple.max_size);
		tuple.setHdr((short)_numColumns, _type, _stringsSizes); 
		
		// insert each column's value into its associated file
		int pos = 0;
		int stringTypesCounter = 0;
		for(int i = 0; i < _numColumns; ++i) {
			switch(_type[i].attrType)
			{
			case AttrType.attrInteger:
				int int_val = tuple.getIntFld(i+1);
				data = new byte[4];
				Convert.setIntValue(int_val, 0, data);
				pos += 4;
				break;
				
			case AttrType.attrString:
				String str_val = tuple.getStrFld(i+1);
				//data = new byte[str_val.length()*2 + 2];
				data = str_val.getBytes();
				break;
				
			case AttrType.attrReal:
				float float_val = tuple.getFloFld(i+1);
				data = new byte[4];
				Convert.setFloValue(float_val, 0, data);
				pos += 4;
				break;
			case AttrType.attrNull:
				break;
			case AttrType.attrSymbol:
				break;
			default:
				//TODO(aalbaltan) throw exception?
				break;
			}
			recordsID[i] = _columnsFiles[i].insertRecord(data);
		}
		
		//TODO(aalbaltan) what does TID's position indicate exactly? and how to track it? 
		// insert TID into header file
		TID tupleId = new TID(_numColumns, 0, recordsID);
		data = new byte[tupleId.getLength()];
		tupleId.writeToByteArray(data, 0);
		_headerFile.insertRecord(data);
		
		return tupleId;
	}
	
	
	/**
	 * Read a tuple from file
	 * 
	 * @param tid
	 *            the ID of the tuple to be read
	 *            
	 * @return tuple object or null
	 * @throws Exception 
	 * @throws HFBufMgrException 
	 * @throws HFDiskMgrException 
	 * @throws HFException 
	 * @throws InvalidTupleSizeException 
	 * @throws InvalidSlotNumberException 
	 * 
	 */
	public Tuple getTuple(TID tid) throws 
	InvalidSlotNumberException, 
	InvalidTupleSizeException, 
	HFException, 
	HFDiskMgrException, 
	HFBufMgrException,
	Exception
	{
		if(tid.getNumRIDs() != _numColumns)
			throw new IllegalArgumentException("invalid tuple ID; number of columns mismatch");
		
		Tuple tuple = new Tuple();
		tuple.setHdr((short)_numColumns, _type, _stringsSizes);
		
		// check whether the tuple exists
		for(int i = 0; i < _numColumns; ++i) {
			Tuple t = _columnsFiles[i].getRecord(tid.getRID(i));
			
			if(t == null)
				return null;
			
			switch(_type[i].attrType)
			{
			case AttrType.attrInteger:
				int int_val = Convert.getIntValue(0, t.getTupleByteArray());
				tuple.setIntFld(i+1, int_val);
				break;
			case AttrType.attrString:
				String str = new String(t.getTupleByteArray());
				tuple.setStrFld(i+1, str);
				break;
			case AttrType.attrReal:
				float float_val = Convert.getFloValue(0, t.getTupleByteArray());
				tuple.setFloFld(i+1, float_val);
				break;
			//TODO(aalbaltan) handle other cases
			default:
				break;
			}
		}
		
		return tuple;
	}
	
	/**
	 * Get tuples count in the file
	 *            
	 * @return number of tuples in this file
	 * @throws IOException 
	 * @throws HFBufMgrException 
	 * @throws HFDiskMgrException 
	 * @throws InvalidTupleSizeException 
	 * @throws InvalidSlotNumberException 
	 * 
	 */
	public int getTupleCnt() throws 
	InvalidSlotNumberException, 
	InvalidTupleSizeException, 
	HFDiskMgrException, 
	HFBufMgrException, 
	IOException
	{
		
		return _headerFile.getRecCnt();
	}
	
	/**
	 * Initiate a sequential scan
	 *            
	 * @return scanned tuples
	 * 
	 */
	public TupleScan openTupleScan() throws 
		InvalidTupleSizeException,
		IOException
	{
		//TODO(aalbaltan) implement 
		/* 
		TupleScan newscan = new TupleScan(this);
		return newscan;
		*/
		return null;
	}
	
	/**
	 * Initiate a sequential scan along a given column
	 * 
	 * @param columnNo
	 * 				index of the column to be scanned [0,numColumns)
	 *            
	 * @return scanned column
	 * 
	 */
	public Scan openColumnScan(int columnNo)
	{
		//TODO(aalbaltan) implement
		return null;
	}
	
	/**
	 * Update a tuple in the file
	 * 
	 * @param tid
	 * 				ID of the tuple to update
	 * @param newTuple
	 * 				the new tuple
	 *            
	 * @return true if a tuple has been updated successfully, false otherwise
	 * 
	 */
	public boolean updateTuple(TID tid, Tuple newTuple)
	{
		if (newTuple == null || tid == null)
			throw new IllegalArgumentException("invalid argument");
		
		boolean bRecordFound = false;
		RID old_rid = new RID();
		
		// look for TID in header file
		try {
			Scan s = _headerFile.openScan();
			
			// look through records in header file
			for(Tuple t = s.getNext(old_rid); t != null; t = s.getNext(old_rid))
			{
				TID t_id = new TID(t.getTupleByteArray());
				if(t_id.equals(tid))
				{
					bRecordFound = true;
					break;
				}
			}
			s.closescan();
		} catch (Exception e) {
			return false;
		}
		
		if(!bRecordFound)
			return false;
		
		// delete record from each column's file
		for(int i = 0; i < _columnsFiles.length; ++i)
		{
			try {
				_columnsFiles[i].deleteRecord(tid.getRID(i));
			} catch (Exception e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		
		try {
			_headerFile.deleteRecord(old_rid);
		}catch(Exception e) {
			e.printStackTrace();
		}
		
		boolean bRecordUpdated = false;
		try {
			//byte[] tupleBytes = tupleToByteArray(newTuple, _type);
			insertTuple(newTuple.getTupleByteArray());
			bRecordUpdated = true;
		}catch(Exception e) {
			e.printStackTrace();
		}
		
		return bRecordUpdated;
	}
	
	/**
	 * Update a single column within a tuple
	 * 
	 * @param tid
	 * 				ID of the tuple to update
	 * @param newtuple
	 * 				the new tuple
	 * @param column
	 * 				the column to update [0, numColumns)
	 *            
	 * @return true if a tuple has been updated successfully, false otherwise
	 * 
	 */
//	public boolean updateColumnofTuple(TID tid, Tuple newTuple, int column)
//	{
//		if(tid == null || newTuple == null)
//			throw new IllegalArgumentException("invalid tid/tuple");
//		
//		if(column < 0 || column >= _numColumns)
//			throw new IndexOutOfBoundsException();
//		
//		boolean bRecordFound = false;
//		RID old_rid = new RID();
//		
//		// look for TID in header file
//		try {
//			Scan s = _headerFile.openScan();
//			
//			// look through records in header file
//			for(Tuple t = s.getNext(old_rid); t != null; t = s.getNext(old_rid))
//			{
//				TID t_id = new TID(t.getTupleByteArray());
//				if(t_id.equals(tid))
//				{
//					bRecordFound = true;
//					break;
//				}
//			}
//			s.closescan();
//		} catch (Exception e) {
//			return false;
//		}
//		
//		if(!bRecordFound) // tuple does not exist
//			return false;
//		
//		RID columnRID = null;
//		boolean bRecordUpdated = false;
//		AttrType[] columnType = new AttrType[1];
//		columnType[0] = _type[column];
//		// delete the record from the specified column
//		try {
//			_columnsFiles[column].deleteRecord(tid.getRID(column));
//			columnRID = _columnsFiles[column].insertRecord(tupleToByteArray(newTuple, columnType));
//			_headerFile.deleteRecord(old_rid);
//			
//			TID newTID = tid;
//			newTID.setRID(column, columnRID);			
//			byte[] data = new byte[newTID.getLength()];
//			newTID.writeToByteArray(data, 0);
//			_headerFile.insertRecord(data);
//			
//			bRecordUpdated = true;
//			
//		} catch (Exception e) {
//			// TODO Auto-generated catch block
//			e.printStackTrace();
//		}
//		
//		return bRecordUpdated;
//	}
	
	
	public boolean updateColumnofTuple(TID tid, Tuple newTuple, int column)
	{
		if(tid == null || newTuple == null)
			throw new IllegalArgumentException("invalid tid/tuple");
		
		if(column < 0 || column >= _numColumns)
			throw new IndexOutOfBoundsException();
		
		boolean bRecordFound = false;
		RID old_rid = new RID();
		
		// look for TID in header file
		try {
			Scan s = _headerFile.openScan();
			
			// look through records in header file
			for(Tuple t = s.getNext(old_rid); t != null; t = s.getNext(old_rid))
			{
				TID t_id = new TID(t.getTupleByteArray());
				if(t_id.equals(tid))
				{
					bRecordFound = true;
					break;
				}
			}
			s.closescan();
		} catch (Exception e) {
			return false;
		}
		
		if(!bRecordFound) // tuple does not exist
			return false;
		
		RID columnRID = null;
		boolean bRecordUpdated = false;
		AttrType[] columnType = new AttrType[1];
		columnType[0] = _type[column];
		// delete the record from the specified column
		try {
			if(!_columnsFiles[column].deleteRecord(tid.getRID(column))) // record not found
				return false;
			columnRID = _columnsFiles[column].insertRecord(tupleToByteArray(newTuple, columnType));
			_headerFile.deleteRecord(old_rid);
			
			TID newTID = new TID(tid.getNumRIDs());
			newTID.copyTid(tid);
			newTID.setRID(column, columnRID);			
			byte[] data = new byte[newTID.getLength()];
			newTID.writeToByteArray(data, 0);
			_headerFile.insertRecord(data);
			
			bRecordUpdated = true;
			
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		return bRecordUpdated;
	}
	
	/* Auxiliary function to convert a tuple into expected byte format */
	private byte[] tupleToByteArray(Tuple t, AttrType[] type) throws 
	FieldNumberOutOfBoundException, 
	IOException
	{
		byte[] buff = new byte[1024];
		int pos = 0;
		for(int i = 0; i < type.length; ++i)
		{
			switch(type[i].attrType)
			{
			case AttrType.attrInteger:
				int int_val = t.getIntFld(i+1);
				Convert.setIntValue(int_val, pos, buff);
				pos += 4;
				break;
			case AttrType.attrString:
				byte[] str = t.getStrFld(i+1).getBytes();
				System.arraycopy(str, 0, buff, 0, str.length);
				pos += str.length;
				break;
			case AttrType.attrReal:
				float float_val = t.getFloFld(i+1);
				Convert.setFloValue(float_val, pos, buff);
				pos += 4;
				break;
			default:
				//TODO(aalbaltan) throw exception?
				break;
			}
		}
		
		byte[] ret_array = new byte[pos];
		System.arraycopy(buff, 0, ret_array, 0, pos);
		return ret_array;
	}
	
}