package global;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.ObjectInputStream;

/**
 * Tuple ID Class
 * 
 * @author Shane
 * 
 */
public class TID {
	// Method readFromByteArray needs to change these class members. Hence, they cannot be final
	private int numRIDs; 
	private int position;
	private RID[] recordIDs;

	/**
	 * Constructs this TID with the given size
	 * 
	 * @param numRIDs
	 *            number of records
	 */
	public TID(int numRIDs) {
		this(numRIDs, 0);
	}

	/**
	 * Constructs this TID with the given size and start position
	 * 
	 * @param numRIDs
	 *            number of records
	 * @param position
	 *            starting position [0, numRIDs - 1]
	 */
	public TID(int numRIDs, int position) {
		this.numRIDs = numRIDs;
		this.recordIDs = new RID[numRIDs];
		this.setPosition(position);

		for (int i = 0; i < numRIDs; i++)
			this.recordIDs[i] = new RID();
	}

	/**
	 * Constructs this TID from an array of RIDs
	 * 
	 * NOTE: all incoming RIDs are copied to new RID objects
	 * 
	 * @param numRIDs
	 *            number of records
	 * @param position
	 *            starting position [0, numRIDs - 1]
	 * @param recordIDs
	 *            RIDs to copy
	 */
	public TID(int numRIDs, int position, RID[] recordIDs) {
		this(numRIDs, position);

		if (recordIDs == null)
			throw new IllegalArgumentException("recordIDs cannot be null");

		if (numRIDs != recordIDs.length)
			// TODO: verify this behavior is appropriate
			throw new IllegalArgumentException(
					"numRIDs must match recordIDs length");

		for (int i = 0; i < numRIDs; i++)
			this.recordIDs[i].copyRid(recordIDs[i]);
	}
	
	/**
	 * Constructs this TID from a byte array
	 * 
	 * @param array
	 *            byte array
	 * @throws IOException 
	 * @throws ClassNotFoundException 
	 */
	public TID(byte[] array) throws IOException, ClassNotFoundException {
		readFromByteArray(array, 0);
	}
	

	/**
	 * Copies the data of the given TID into this TID
	 * 
	 * @param tid
	 *            the TID to copy into this TID
	 */
	public void copyTid(TID tid) {
		if (numRIDs != tid.numRIDs)
			throw new IllegalArgumentException(
					"Cannot copy a TID of a different size");

		position = tid.position;
		for (int i = 0; i < numRIDs; i++)
			recordIDs[i].copyRid(tid.recordIDs[i]);
	}

	/**
	 * Returns whether the given TID matches this TID
	 * 
	 * @param tid
	 *            the TID to compare too
	 * @return whether or not this TID and the other TID are equivalent
	 */
	public boolean equals(TID tid) {
		// TODO: ensure the position field should be considered for this
		// start by checking basic fields
		if (numRIDs == tid.numRIDs && position == tid.position) {
			// check each RIDs individually
			for (int i = 0; i < numRIDs; i++)
				if (!recordIDs[i].equals(tid.recordIDs[i]))
					return false; // there exists an RID doesn't match

			return true; // all RIDs match
		}
		return false;
	}

	/**
	 * Write the rid into a byte array at offset
	 * 
	 * @param ary
	 *            the specified byte array
	 * @param offset
	 *            the offset of byte array to write
	 * @exception java.io.IOException
	 *                I/O errors
	 */
	public void writeToByteArray(byte[] array, int offset) throws IOException {
		// base data, 2 integers, 8 bytes total
		Convert.setIntValue(numRIDs, offset, array);
		Convert.setIntValue(position, offset + 4, array);

		// record ID data, 8 bytes per record
		for (int i = 0; i < recordIDs.length; i++)
			recordIDs[i].writeToByteArray(array, 8 * i + offset + 8);
	}

	/**
	 * Gets the size of this TID
	 * 
	 * @return numRIDs
	 */
	public int getNumRIDs() {
		return numRIDs;
	}

	/**
	 * Gets the position of this TID
	 * 
	 * @return position
	 */
	public int getPosition() {
		return position;
	}

	/**
	 * Sets the position of this TID
	 * 
	 * @param position
	 *            the position in range [0, numRIDs - 1]
	 */
	public void setPosition(int position) {
		if (position < 0 || position >= numRIDs)
			// TODO: verify this behavior is appropriate
			throw new IllegalArgumentException(
					"expected position in range [0, numRIDs - 1]");

		this.position = position;
	}

	/**
	 * Copies the RID at the specified index to the specified value via copying
	 * 
	 * @param column
	 * @param recordID
	 */
	public RID getRID(int column) {
		// TODO: should we let invalid range errors propagate themselves, or
		// make our own?
		return recordIDs[column];
	}

	/**
	 * Copies the RID at the specified index to the specified value via copying
	 * 
	 * @param column
	 * @param recordID
	 */
	public void setRID(int column, RID recordID) {
		// TODO: should we let invalid range errors propagate themselves, or
		// make our own?
		recordIDs[column].copyRid(recordID);
	}
	
	/**
	 * Get TID length. 
	 * 
	 * @param column
	 * @param recordID
	 */
	public int getLength(){
		// numRIDs (4 bytes) + position (4 bytes) + number of records * record length (8 bytes)
		return 8 + recordIDs.length * 8;
	}
	
	/**
	 * Read TID information from byte array
	 * 
	 * @param array
	 *            the specified byte array
	 * @param offset
	 *            the offset of byte array to read
	 * @exception java.io.IOException
	 *                I/O errors
	 * @throws ClassNotFoundException 
	 */
	public void readFromByteArray(byte[] array, int offset) throws IOException, ClassNotFoundException {
		// base data, 2 integers, 8 bytes total
		numRIDs = Convert.getIntValue(offset, array);
		position = Convert.getIntValue(offset + 4, array);

		// record ID data, 8 bytes per record
		recordIDs = new RID[numRIDs];
		int pos = offset + 8;
		for(int i = 0; i < numRIDs; ++i)
		{
			recordIDs[i] = new RID();
			recordIDs[i].readFromByteArray(array, pos + i * 8);
		}
	}


}
