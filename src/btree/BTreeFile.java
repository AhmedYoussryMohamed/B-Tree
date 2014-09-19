package btree;

import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedList;

import chainexception.ChainException;

import bufmgr.BufMgrException;
import bufmgr.BufferPoolExceededException;
import bufmgr.HashEntryNotFoundException;
import bufmgr.HashOperationException;
import bufmgr.InvalidBufferException;
import bufmgr.InvalidFrameNumberException;
import bufmgr.PageNotReadException;
import bufmgr.PagePinnedException;
import bufmgr.PageUnpinnedException;
import bufmgr.ReplacerException;

import diskmgr.DiskMgrException;
import diskmgr.FileEntryNotFoundException;
import diskmgr.FileIOException;
import diskmgr.InvalidPageNumberException;
import diskmgr.Page;
import global.*;
import heap.HFPage;
import heap.InvalidSlotNumberException;

public class BTreeFile extends IndexFile implements GlobalConst {
	private BTreeHeaderPage headerPage;
	private String fileName = "";
	private ArrayList<Integer> listPageIds = new ArrayList<Integer>();

	public BTreeFile(String fileName) throws FileIOException, ChainException,
			IOException {

		PageId headerPageId = SystemDefs.JavabaseDB.get_file_entry(fileName);
		if (headerPageId != null) {
			HFPage temp = new HFPage();
			temp.setCurPage(headerPageId);
			headerPage = new BTreeHeaderPage( headerPageId ,(short) Convert.getIntValue(4, temp.getHFpageArray() ) );
			this.fileName = fileName;
		}// end if
		else {
			
			// throw error.
		}

	}// end constructor.

	public BTreeFile(String fileName, int keytype, int keysize,
			int delete_fashion) throws FileIOException, ChainException,
			IOException {

		this.fileName = fileName;
		PageId headerPageId = SystemDefs.JavabaseDB.get_file_entry(fileName);
		if (headerPageId == null) {
			HFPage temp = new HFPage();
//			headerPageId = SystemDefs.JavabaseBM.newPage(temp, 1);
			headerPage = new BTreeHeaderPage(keytype);
			headerPageId = headerPage.getCurPage();
			SystemDefs.JavabaseDB.add_file_entry(fileName, headerPageId);
			
//			temp.init(headerPageId, temp);
//			headerPage = new BTreeHeaderPage(headerPageId ,keytype);
			
			headerPage.setLength(keysize);
			BTLeafPage root = new BTLeafPage(keytype);
			headerPage.setRootId( root.getCurPage().pid );
			headerPage.setTypeKey(keytype);
			headerPage.setCurPage( headerPageId );
			headerPage.setNextPage( root.getCurPage() );
			
			SystemDefs.JavabaseBM.unpinPage(root.getCurPage(), true);
		} else {
			HFPage temp = new HFPage();
			temp.setCurPage(headerPageId);
			headerPage = (BTreeHeaderPage) temp;
		}
	}// end constructor.

	@Override
	public void insert(KeyClass key, RID rid) throws IOException,
			ConstructPageException, KeyNotMatchException,
			NodeNotMatchException, ConvertException,
			InvalidSlotNumberException, InsertRecException, DeleteRecException,
			BufferPoolExceededException, HashOperationException,
			ReplacerException, HashEntryNotFoundException,
			InvalidFrameNumberException, PagePinnedException,
			PageUnpinnedException, PageNotReadException, BufMgrException,
			DiskMgrException {

		PageId curId = new PageId(headerPage.get_rootId().pid);
		BTSortedPage sortedPage = new BTSortedPage(curId, headerPage.get_keyType());
		listPageIds = new ArrayList<Integer>();
		while (sortedPage.getType() != NodeType.LEAF) {
			//lawo leaf msh hayt7at.
			listPageIds.add( curId.pid );
			BTIndexPage indexPage = new BTIndexPage(curId, headerPage.get_keyType());
			PageId nextId = indexPage.getPageNoByKey(key);
			SystemDefs.JavabaseBM.unpinPage(curId, true);
			sortedPage = new BTSortedPage(nextId, headerPage.get_keyType());
			curId.pid = nextId.pid;
			SystemDefs.JavabaseBM.unpinPage(nextId, true);
			
		}// end while.
		
		//ana ba3et el leaf!!
//		SystemDefs.JavabaseBM.unpinPage(curId, true);
		insertRecursive( curId.pid, key, rid, null, listPageIds.size() - 1 );

	}// end method.

	// law hab3at el rid yb2a el pid b null wel 3aks sa7e7.. w kda kda ana 3amel
	// if awl mabd5ol el method.
	public void insertRecursive(int pgid, KeyClass key, RID rid, PageId pid ,int index) throws InsertRecException, IOException,
			InvalidSlotNumberException, KeyNotMatchException,
			NodeNotMatchException, ConvertException, DeleteRecException,
			ConstructPageException, BufferPoolExceededException,
			HashOperationException, ReplacerException,
			HashEntryNotFoundException, InvalidFrameNumberException,
			PagePinnedException, PageUnpinnedException, PageNotReadException,
			BufMgrException, DiskMgrException {
		
		BTSortedPage sortedPage = new BTSortedPage( new PageId(pgid), headerPage.get_keyType() );
		
		if (sortedPage.getType() == NodeType.LEAF) {
			SystemDefs.JavabaseBM.unpinPage( new PageId(pgid), true);
			BTLeafPage leafPage = new BTLeafPage( new PageId(pgid), headerPage.get_keyType() );
			RID returnId = leafPage.insertRecord(key, rid);
			
			if (returnId == null) {// this means there is no space for the added
									// record
				// Now making a list of the records in the leaf + record to be
				// added.
				LinkedList<KeyDataEntry> list = new LinkedList<KeyDataEntry>();
				RID idTemp = new RID();
				boolean added = false;
				KeyDataEntry firstKeyDataEntry = leafPage.getFirst(idTemp);
				KeyClass data2 = firstKeyDataEntry.key;
				if (BT.keyCompare(key, data2) < 0) {
					KeyDataEntry k = new KeyDataEntry(key, rid);
					list.add(k);
					added = true;
				}
				list.add(firstKeyDataEntry);
				// idTemp ->first entry.
				int numOfRecords = leafPage.getSlotCnt();
				for (int i = 1; i < numOfRecords; i++) {
					KeyDataEntry curKeyDataEntry = leafPage.getNext(idTemp);
					data2 = curKeyDataEntry.key;
					if (BT.keyCompare(key, data2) < 0 && !added) {
						KeyDataEntry k = new KeyDataEntry(key, rid);
						list.add(k);
						added = true;
					}
					list.add(curKeyDataEntry);
				}// end for.
				if (!added) {
					KeyDataEntry k = new KeyDataEntry(key, rid);
					list.add(k);
				}
				// The list now contains the n+1 records sorted.

				// First delete all the records from the leafPage.
				// el records et3'yaret!!!!
				for (int i = 0; i <= numOfRecords; i++) {
					leafPage.delEntry(list.get(i));
				}
				// Then insert half the records in the original LeafPage.
				int half = numOfRecords / 2;
				for (int i = 0; i < half; i++) {
					leafPage.insertRecord( list.get(i) );
				}

				// Then make a new leafPage and insert the second half of the
				// records in it.
				HFPage temp = new HFPage();
				
				BTLeafPage newLeafPage = new BTLeafPage( headerPage.get_keyType() );
				for (int i = half; i <= numOfRecords; i++) {
					newLeafPage.insertRecord( list.get(i) );
				}// end for(i).
				
				KeyClass newKeyToBeAdded = list.get(half).key;
				
				PageId nextId = sortedPage.getNextPage();
				
				if( nextId.pid != -1 ){
					BTLeafPage nextLeaf = new BTLeafPage( nextId, headerPage.get_keyType() );
					nextLeaf.setPrevPage( newLeafPage.getCurPage() );
					newLeafPage.setNextPage( nextLeaf.getCurPage() );
					SystemDefs.JavabaseBM.unpinPage( nextId, true);
				}//end if.
				
				sortedPage.setNextPage(newLeafPage.getCurPage());
				newLeafPage.setPrevPage( sortedPage.getCurPage() );
				
				
				
				if( index != -1 ){
				 insertRecursive( listPageIds.get(index), newKeyToBeAdded, null ,newLeafPage.getCurPage() ,index - 1);
				}else{
					BTIndexPage newRoot = new BTIndexPage( headerPage.get_keyType() );
					headerPage.setRootId( newRoot.getCurPage().pid );
					headerPage.setNextPage(newRoot.getCurPage());
					newRoot.setLeftLink( leafPage.getCurPage() );
					newRoot.insertKey(newKeyToBeAdded, newLeafPage.getCurPage());
					SystemDefs.JavabaseBM.unpinPage( newRoot.getCurPage(), true);
				}//end else.
				SystemDefs.JavabaseBM.unpinPage( newLeafPage.getCurPage(), true);
			}//end null.
			SystemDefs.JavabaseBM.unpinPage( new PageId(pgid), true);
		} else {//ya3ne indexPage
			
			SystemDefs.JavabaseBM.unpinPage( new PageId(pgid), true);
			BTIndexPage indexPage = new BTIndexPage( new PageId(pgid), headerPage.get_keyType() );
			RID returnId = indexPage.insertKey(key, pid);
			if (returnId == null) {// this means there is no space for the added
									// record
				// Now making a list of the records in the leaf + record to be
				// added.
				LinkedList<KeyDataEntry> list = new LinkedList<KeyDataEntry>();
				RID idTemp = new RID();
				boolean added = false;
				KeyDataEntry firstKeyDataEntry = indexPage.getFirst(idTemp);
				KeyClass data2 = firstKeyDataEntry.key;
				if (BT.keyCompare(key, data2) < 0) {
					KeyDataEntry k = new KeyDataEntry(key, pid);
					list.add(k);
					added = true;
				}
				list.add(firstKeyDataEntry);
				// idTemp ->first entry.
				int numOfRecords = indexPage.getSlotCnt();
				// msh lazem ne3mel compare aslan.
				for (int i = 1; i < numOfRecords; i++) {
					KeyDataEntry curKeyDataEntry = indexPage.getNext(idTemp);
					data2 = curKeyDataEntry.key;
					if (BT.keyCompare(key, data2) < 0 && !added) {
						//ezaey el key data entry rid????
						KeyDataEntry k = new KeyDataEntry(key, pid);
						list.add(k);
						added = true;
					}
					list.add(curKeyDataEntry);
				}// end for.
				if (!added) {
					KeyDataEntry k = new KeyDataEntry(key, pid);
					list.add(k);
				}
				// The list now contains the n+1 records sorted.

				//now dealing with the sortedPage to be able to delte and insert record in indexpage.
				// First delete all the records from the indexPage.
				
				RID tempId = new RID();
				indexPage.getFirst(tempId);
				for (int i = 0; i <= numOfRecords; i++) {
					// mesh 3arf a3ml delete ezay :/
//					sortedPage.deleteSortedRecord(tempId);
//					indexPage.getNext(tempId);
					indexPage.delEntry(list.get(i));
				}
				// Then insert half the records in the original IndexPage.
				int half = numOfRecords / 2;
				for (int i = 0; i < half; i++) {
					indexPage.insertRecord( list.get(i) );
				}

				// Then make a new leafPage and insert the second half of the
				// records in it.
				HFPage temp = new HFPage();
				// kda sa7????
				
				BTIndexPage newIndexPage = new BTIndexPage(headerPage.get_keyType());
				for (int i = half; i <= numOfRecords; i++) {
					newIndexPage.insertRecord(list.get(i));
				}// end for(i).
				
				KeyClass newKeyToBeAdded = list.get(half).key;
				
				
				if( index != -1){
					insertRecursive(  listPageIds.get(index) , newKeyToBeAdded, null, newIndexPage.getCurPage() ,index - 1);
				}else{
					// a3ml new level now.
					BTIndexPage newRoot = new BTIndexPage( headerPage.get_keyType() );
					headerPage.setRootId( newRoot.getCurPage().pid );
					headerPage.setNextPage(newRoot.getCurPage());
					newRoot.setLeftLink( indexPage.getCurPage() );
					newRoot.insertKey(newKeyToBeAdded, newIndexPage.getCurPage());
					SystemDefs.JavabaseBM.unpinPage( newRoot.getCurPage(), true);
				}//end else index.
				SystemDefs.JavabaseBM.unpinPage( newIndexPage.getCurPage(), true);
			}//end if return id.
			SystemDefs.JavabaseBM.unpinPage( new PageId(pgid), true);
		}//end else index or leaf.
//		SystemDefs.JavabaseBM.unpinPage( new PageId(pgid), true);
	}// end method.

	@Override
	public boolean Delete(KeyClass data, RID rid) throws IOException,
			ConstructPageException, KeyNotMatchException,
			NodeNotMatchException, ConvertException, InvalidSlotNumberException, DeleteRecException, ReplacerException, PageUnpinnedException, HashEntryNotFoundException, InvalidFrameNumberException {
		if(headerPage == null){
			return false;
		}
		PageId curId = new PageId(headerPage.get_rootId().pid );
		BTSortedPage sortedPage = new BTSortedPage(curId,
				headerPage.get_keyType());
		
		while (sortedPage.getType() == NodeType.INDEX) {
			BTIndexPage indexPage = new BTIndexPage(curId , headerPage.get_keyType());
			PageId nextId = indexPage.getPageNoByKey(data);
			sortedPage = new BTSortedPage(nextId, headerPage.get_keyType());
			curId.pid=nextId.pid;
		}// end while.
		BTLeafPage leafPage = new BTLeafPage(curId , headerPage.get_keyType());
		KeyDataEntry keyEntry = new KeyDataEntry(data, rid);
		boolean del = leafPage.delEntry(keyEntry);
		SystemDefs.JavabaseBM.unpinPage(curId, true);
		return del;
	}// end method.
	
	public void destroyFile() throws IOException, FileIOException,
				InvalidPageNumberException, DiskMgrException, ReplacerException,
				HashOperationException, PageUnpinnedException,
				InvalidFrameNumberException, PageNotReadException,
				BufferPoolExceededException, PagePinnedException, BufMgrException,
				ConstructPageException, KeyNotMatchException,
				NodeNotMatchException, ConvertException,
				InvalidSlotNumberException, HashEntryNotFoundException,
				FileEntryNotFoundException, InvalidBufferException {
			
			// check if header page is not null
			if (headerPage != null) {
			
				// check if rootId is valid
				PageId rootID = new PageId(headerPage.get_rootId().pid);
				if (rootID.pid != GlobalConst.INVALID_PAGE) {
					// call the recursive Function
					destroyRecursive(rootID);
				}
				SystemDefs.JavabaseDB.delete_file_entry(fileName);
				headerPage = null;
			}
			
	}// end method.
	
	public void destroyRecursive(PageId pid) throws IOException,
		ReplacerException, HashOperationException, PageUnpinnedException,
		InvalidFrameNumberException, PageNotReadException,
		BufferPoolExceededException, PagePinnedException, BufMgrException,
		HashEntryNotFoundException, InvalidBufferException,
		DiskMgrException, ConstructPageException, KeyNotMatchException,
		NodeNotMatchException, ConvertException, InvalidSlotNumberException {
			Page page = new Page();
//			SystemDefs.JavabaseBM.pinPage(pid, page, false);
			BTSortedPage sortedpage = new BTSortedPage( pid, headerPage.get_keyType());
			
			if (sortedpage.keyType == NodeType.LEAF) {
				SystemDefs.JavabaseBM.unpinPage(pid, true);
				SystemDefs.JavabaseBM.freePage(pid);
			} else {
				SystemDefs.JavabaseBM.unpinPage(pid, true);
				BTIndexPage indexPage = new BTIndexPage(pid, headerPage.get_keyType());
				if( indexPage.getLeftLink().pid != -1){
					destroyRecursive(indexPage.getLeftLink());
				}
				KeyDataEntry dataEntry;
				RID ridTemp = new RID();
				PageId nextPid = new PageId();
				for (dataEntry = indexPage.getFirst(ridTemp); dataEntry != null; dataEntry = indexPage
						.getNext(ridTemp)) {
					nextPid = ((IndexData) (dataEntry.data)).getData();
					destroyRecursive(nextPid);
					// Unpining and freeing every page.
					SystemDefs.JavabaseBM.unpinPage(pid, true);
					SystemDefs.JavabaseBM.freePage(pid);
				}
			}// end elseif.
		}// end method.
	
	public void close() throws ReplacerException, PageUnpinnedException,
			HashEntryNotFoundException, InvalidFrameNumberException,
			IOException {
		if( headerPage != null){
			SystemDefs.JavabaseBM.unpinPage(headerPage.getCurPage(), true);
		}
	}// end method.
	
	public BTFileScan new_scan(KeyClass lo_key, KeyClass hi_key)
			throws ConstructPageException, IOException, ChainException {
		BTFileScan scan = new BTFileScan(this);
		scan.setLowKey(lo_key);
		scan.setHighKey(hi_key);
		return scan;
	}// end method.

	public BTreeHeaderPage getHeaderPage() {
		return this.headerPage;
	}// end method.

	public void traceFilename(String string) {
		// TODO Auto-generated method stub
		
	}


}// end class.
