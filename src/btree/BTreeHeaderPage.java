package btree;

import java.io.IOException;

import diskmgr.Page;

import global.*;

public class BTreeHeaderPage extends BTSortedPage {
	
	public BTreeHeaderPage(int paramInt) throws ConstructPageException, IOException {
		super(paramInt);
		this.setType(NodeType.BTHEAD);
	}
	
	public BTreeHeaderPage(Page paramPage, int paramInt) throws IOException{
		super(paramPage,paramInt);
		this.setType(NodeType.BTHEAD);
	}
	
	public BTreeHeaderPage(PageId paramPage, int paramInt) throws ConstructPageException, IOException{
		super(paramPage,paramInt);
		this.setType(NodeType.BTHEAD);
	}
	
	public void setRootId( int num ) throws IOException{
		this.setNextPage(new PageId(num));
		Convert.setIntValue( num, 0, data);
	}//end method.
	
	public void setTypeKey( int num ) throws IOException{
		Convert.setIntValue( num, 4, data);
	}//end method.
	
	public void setLength( int num ) throws IOException{
		Convert.setIntValue( num, 8, data);
	}//end method.
	
	public PageId get_rootId() throws IOException{
		return this.getNextPage();
	}
	
	public short get_keyType() throws IOException{
		return (short) Convert.getIntValue(4, data);
	}
	
	public short get_keySize() throws IOException{
		return (short) Convert.getIntValue(8, data);
	}
	
	
}//end class.
