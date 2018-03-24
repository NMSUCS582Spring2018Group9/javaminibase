package tests;

import global.*;

class TupleTest extends TestDriver {

	public TupleTest() {
		super("tupletest");
	}
	
	TID tid;
	RID[] rids;
	
	public void before() {
		rids = new RID[] { new RID(new PageId(3), 4), new RID(new PageId(1), 2), new RID(new PageId(5), 7),
				new RID(new PageId(8), 1), };

		tid = new TID(4, 3, rids);
		assert tid.getNumRIDs() == 4;
		assert tid.getPosition() == 3;
	}

	public String testName() {
		return "tuple";
	}

	public boolean test1() {

		System.out.println("Test 1 ensures each constructor functions as expected:");

		System.out.println("Signature: public TID(int numRIDs)");

		try {
			TID tid = new TID(4);
			assert tid.getNumRIDs() == 4;
			assert tid.getPosition() == 0;
			for (int i = 0; i < 4; i++)
				assert tid.getRID(i) != null;
		} catch (Exception e) {
			e.printStackTrace();
			return FAIL;
		}

		System.out.println("Signature: public TID(int numRIDs, int position)");
		try {
			TID tid = new TID(4, 3);
			assert tid.getNumRIDs() == 4;
			assert tid.getPosition() == 3;
			for (int i = 0; i < 4; i++)
				assert tid.getRID(i) != null;
		} catch (Exception e) {
			e.printStackTrace();
			return FAIL;
		}

		System.out.println("Signature: public TID(int numRIDs, int position, RID[] recordIDs)");
		try {
			RID[] rids = new RID[] { new RID(new PageId(3), 4), new RID(new PageId(1), 2), new RID(new PageId(5), 7),
					new RID(new PageId(8), 1), };

			TID tid = new TID(4, 3, rids);
			assert tid.getNumRIDs() == 4;
			assert tid.getPosition() == 3;

			for (int i = 0; i < 4; i++)
				assert tid.getRID(i).equals(rids[i]);
		} catch (Exception e) {
			e.printStackTrace();
			return FAIL;
		}
		return OK;
	}

	public boolean test2() {
		before();
		System.out.println("Test 2 ensures each setter functions as expected:");

		System.out.println("Setter for position...");
		try {
			for (int i = 0; i < 4; i++) {
				tid.setPosition(i);
				assert tid.getPosition() == i;
			}
		} catch (Exception e) {
			e.printStackTrace();
			return FAIL;
		}

		Exception except = null;
		try {
			tid.setPosition(-1);
		} catch (Exception e) {
			except = e;
		}
		if (except == null) {
			System.out.println("Expected IllegalArgumentException, got: " + except);
			return FAIL;
		}

		except = null;
		try {
			tid.setPosition(4);
		} catch (Exception e) {
			except = e;
		}
		if (except == null) {
			System.out.println("Expected IllegalArgumentException, got: " + except);
			return FAIL;
		}

		return OK;
	}

	public boolean test3() {
		before();
		System.out.println("Test 3 RIDs are always copied:");

		try {
			rids[0].slotNo = 1;
			assert !tid.getRID(0).equals(rids[0]);
		} catch (Exception e) {
			e.printStackTrace();
			return FAIL;
		}

		try {
			tid.setRID(0, rids[0]);
			assert tid.getRID(0).equals(rids[0]);
			rids[0].slotNo = 2;
			assert !tid.getRID(0).equals(rids[0]);
		} catch (Exception e) {
			e.printStackTrace();
			return FAIL;
		}

		try {
			RID rid = tid.getRID(0);
			assert tid.getRID(0).equals(rid);
			rid.slotNo = 3;
			assert !tid.getRID(0).equals(rid);
		} catch (Exception e) {
			e.printStackTrace();
			return FAIL;
		}

		return OK;
	}

	public boolean test4() {
		before();
		System.out.println("Test 4 equality:");
		
		assert tid.equals(tid);
		
		return OK;
	}

	public static void main(String[] args) {
		try {
			TupleTest test = new TupleTest();
			test.runTests();
		} catch (Exception e) {
			e.printStackTrace();
			System.err.println("Error encountered during Tuple ID tests:\n");
			Runtime.getRuntime().exit(1);
		}
	}
}