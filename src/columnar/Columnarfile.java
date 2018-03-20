package columnar;

import java.io.IOException;

import diskmgr.Page;
import global.*;
import heap.*;

//TODO(aalbaltan) to be replaced later
// dummy classes
class TupleScan{
	
}

class Scan{
	
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
	
	// TODO(aalbaltan) phase 2 document defines numColumns as static int. This means all columnar
	// files created are required to have the same number of columns which does not make a whole lot of sense. 
	
	/* Number of columns/attributes of this relation */
	static int _numColumns;
	
	/* Type of each column/attribute */
	AttrType[] _type;
	
	/* Columanrfile header file. The entries in the header are of type TID. */ 
	Heapfile _headerFile;
	
	/* A heapfile for each column in the columnarfile */
	Heapfile[] _columnsFiles;
	
	int _ftype;
	
	/* A flag to track file status */ 
	private boolean _file_deleted;
	
	/* Columnarfile name*/
	private String _fileName;
	private static int tempfilecount = 0;
	
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
	public Columnarfile(String name, int numColumns, AttrType[] type) throws 
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
	 */
	// TODO(aalbaltan) project's phase 2 instructions state that this function must take a byte[] as an argument. However, I couldn't find
	// a way to implement the function with the suggested argument type. I changed the argument type to Tuple. Change later if this is not
	// OK.
	public TID insertTuple(byte[] tuplePtr) throws
	//public TID insertTuple(Tuple tuplePtr) throws
		IllegalArgumentException, 
		FieldNumberOutOfBoundException, 
		IOException, 
		InvalidSlotNumberException, 
		InvalidTupleSizeException, 
		SpaceNotAvailableException, 
		HFException, 
		HFBufMgrException, 
		HFDiskMgrException
	{
		if(tuplePtr.length == 0)
			throw new IllegalArgumentException("cannot insert empty tuple");
		
		if(tuplePtr.length > Tuple.max_size)
			throw new IllegalArgumentException("tuple size too big");
		
		RID[] recordsID = new RID[_numColumns];
		byte[] data = null;
		
		// insert each column's value into its associated file
		int pos = 0;
		for(int i = 0; i < _numColumns; ++i) {
			switch(_type[i].attrType)
			{
			case AttrType.attrInteger:
				int int_val = Convert.getIntValue(pos, tuplePtr);
				data = new byte[4];
				Convert.setIntValue(int_val, 0, data);
				pos += 4;
				break;
				
			case AttrType.attrString:
				short str_len = Convert.getShortValue(pos, tuplePtr);
				if(str_len <= 0) {
					//TODO(aalbaltan) throw exception?
				}
				pos += 2;
				String str_val = Convert.getStrValue(pos, tuplePtr, str_len*2); // account of nulls?
				pos += str_len;
				data = new byte[str_val.length()*2 + 2];
				Convert.setShortValue(str_len, 0, data);
				Convert.setStrValue(str_val, 2, data);
				break;
				
				//TODO(aalbaltan) implement
			case AttrType.attrReal:
				float float_val = Convert.getFloValue(pos, tuplePtr);
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
		
		// insert TID into header file
		//TODO(aalbaltan) what does TID's position indicate exactly? and how to track it? 
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
		
		// check whether the tuple exists
		for(int i = 0; i < _numColumns; ++i) {
			Tuple t = new Tuple();
			t = _columnsFiles[i].getRecord(tid.getRID(i));
			
			if(t == null)
				return null;
			
			switch(_type[i].attrType)
			{
			case AttrType.attrInteger:
				int int_val = t.getIntFld(1);
				break;
			case AttrType.attrString:
				String str = t.getStrFld(1);
				break;
			case AttrType.attrReal:
				float float_val = t.getFloFld(1);
				break;
			//TODO(aalbaltan) handle other cases
			default:
				break;
			}
		}
		
		return null;
	}
	
	/**
	 * Get tuples count in the file
	 *            
	 * @return number of tuples in this file
	 * 
	 */
	public int getTupleCnt()
	{
		//TODO(aalbaltan) implement
		return 0;
	}
	
	/**
	 * Initiate a sequential scan
	 *            
	 * @return scanned tuples
	 * 
	 */
	public TupleScan openTupleScan()
	{
		//TODO(aalbaltan) implement
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
	boolean updateTuple(TID tid, Tuple newtuple)
	{
		//TODO(aalbaltan) implement 
		return false;
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
	boolean updateColumnofTuple(TID tid, Tuple newtuple, int column)
	{
		//TODO(aalbaltan) implement
		return false;
	}
	
}