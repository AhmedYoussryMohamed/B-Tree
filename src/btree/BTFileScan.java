package btree;

import java.io.IOException;
import java.util.ArrayList;

import chainexception.ChainException;

import bufmgr.HashEntryNotFoundException;
import bufmgr.InvalidFrameNumberException;
import bufmgr.PageUnpinnedException;
import bufmgr.ReplacerException;

import global.*;
import heap.InvalidSlotNumberException;

public class BTFileScan extends IndexFileScan implements GlobalConst {
	private RID current = new RID();
	private RID next = new RID();
	private BTLeafPage leaf;
	private BTreeHeaderPage headerPage;
	private KeyClass lowKey = null;
	private KeyClass highKey = null;
	private boolean destroyed = false;
	
	public BTFileScan( BTreeFile bTree ) throws IOException, ConstructPageException, ChainException {
		
		headerPage = bTree.getHeaderPage();
		
//		int pid = headerPage.get_rootId().pid;
//		PageId pgid = new PageId(pid);
//		BTSortedPage tempPage;
//		while( pgid.pid != -1 ){
//			tempPage = new BTSortedPage( pgid, headerPage.get_keyType() );
////			tempPage.dumpPage();
//			if( tempPage.getType() == NodeType.INDEX ){
//				SystemDefs.JavabaseBM.unpinPage(pgid, true);
//				BTIndexPage indexPage = new BTIndexPage( headerPage.get_keyType() );
//				pgid =  indexPage.getLeftLink();
//				SystemDefs.JavabaseBM.unpinPage( indexPage.getCurPage(), true );
//			}//end if.
//			else if( tempPage.getType() == NodeType.LEAF ){
//				//ma3mltsh unpin:
//				SystemDefs.JavabaseBM.unpinPage(pgid, true);
//				leaf = new BTLeafPage( headerPage.get_keyType() );
//				RID rid = new RID();
//				leaf.getFirst(rid);
//				current.copyRid(rid);
//				next.copyRid(rid);
//				break;
//			}//end else if.
//
//		}//end while
		
			PageId curId = new PageId(headerPage.get_rootId().pid);
			BTSortedPage sortedPage = new BTSortedPage(curId, headerPage.get_keyType());
			while (sortedPage.getType() != NodeType.LEAF) {
				//lawo leaf msh hayt7at.
				BTIndexPage indexPage = new BTIndexPage(curId, headerPage.get_keyType());
				PageId nextId = indexPage.getLeftLink();
				SystemDefs.JavabaseBM.unpinPage(curId, true);
				sortedPage = new BTSortedPage(nextId, headerPage.get_keyType());
				curId.pid = nextId.pid;
				SystemDefs.JavabaseBM.unpinPage(nextId, true);
				
			}// end while.
			
			leaf = new BTLeafPage( curId, headerPage.get_keyType() );
			RID rid = new RID();
			leaf.getFirst(rid);
			current.copyRid(rid);
			next.copyRid(rid);
		// ana hena ma3aya awel record fe 2awel leaf.
		
		
		if( current == null ){
			//throw error.
		}
		
	}//end Constructor.
	
	@Override
	public KeyDataEntry get_next() {
		if( !destroyed ){
			if( current != null ){
				current.copyRid(next);
				try {
					next = leaf.nextRecord(next);
				} catch (IOException e) {
					e.printStackTrace();
				}
				
			}//end if.
			else{
				 try {
					 PageId nextPageId = leaf.getNextPage();
					 if( nextPageId.pid != -1){
						SystemDefs.JavabaseBM.unpinPage(leaf.getCurPage(), true);
						SystemDefs.JavabaseBM.pinPage( nextPageId, leaf, false);
						leaf.dumpPage();
						leaf.getFirst(current);
						next.copyRid(current);
						next = leaf.nextRecord(next);
					 }else{
						 return null;
					 }
				} catch (ChainException
						| IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
				
			}//end else.
			KeyDataEntry toBeReturned = null;
			try {
				toBeReturned = leaf.getCurrent(current);
				if( BT.keyCompare(highKey, toBeReturned.key) < 0 ){
					return null;
				}
				
			} catch (ChainException| IOException e) {
				e.printStackTrace();
			}
			return toBeReturned;
		}//end if destroyed.
		return null;
		
	}//end method.

	@Override
	public void delete_current() {
		
		if( current != null && !destroyed ){
			try {
				KeyDataEntry dataEntry = leaf.getCurrent(current);
				leaf.delEntry(dataEntry);
			} catch (ChainException | IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			
		}//end if.
		
	}//end method.

	@Override
	public int keysize() {
		if( !destroyed){
			try {
				return headerPage.get_keySize();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}//end if.
		return -1;
	}//end method.
	
	public void destroyBTreeFileScan() throws ChainException, IOException{
		destroyed = true;
		SystemDefs.JavabaseBM.unpinPage(leaf.getCurPage(), true);
	}//end method.

	public void setHighKey(KeyClass high_key) {
	        this.highKey = high_key;
	}
	

	public void setLowKey(KeyClass low_key) throws ChainException, IOException {
		//3ayez a7rak el lowkey
		this.lowKey = low_key;
		RID rid = new RID();
		
		KeyDataEntry dEntry = leaf.getFirst(rid);
		KeyClass curKey = dEntry.key;
		
		while( BT.keyCompare(curKey, low_key) != 0 ){
			dEntry = leaf.getNext(rid);
			if( dEntry != null ){
				curKey = dEntry.key;
			}else{
				PageId nextPageId = leaf.getNextPage();
				if( nextPageId.pid != -1){
					SystemDefs.JavabaseBM.unpinPage(leaf.getCurPage(), true);
					SystemDefs.JavabaseBM.pinPage( nextPageId, leaf, false);
					leaf.dumpPage();
					dEntry = leaf.getFirst(rid);
					curKey = dEntry.key;
				}else{
					//wasal le 2a5er 7aga:
					System.out.println("Invalid lower keyyyyyyy.");
					break;
				}//end else.
				
			}//end else.
			
		}//end while.
		
		
	}//end method.
	
	public KeyClass getLowKey() {
        return lowKey;
	}
	
	public KeyClass getHigh_key() {
	        return highKey;
	}
	
	
}//end class.
