package edu.umn.cs.Nebula.test;

import static org.junit.Assert.assertTrue;

import org.junit.Test;

import edu.umn.cs.Nebula.model.NodeInfo;
import edu.umn.cs.Nebula.model.NodeType;
import edu.umn.cs.Nebula.model.QuadTree;

public class QuadTreeTest {
	private NodeInfo root = new NodeInfo("root", "127.0.0.1", 0, 0, NodeType.COMPUTE);
	private QuadTree tree = new QuadTree(0, 3, -1, root);

	@Test
	public void initTree() {
		assertTrue(tree.getRoot().getScore() == 0);
	}

	@Test
	public void testInsert() {
		NodeInfo node1 = new NodeInfo("child1", "127.0.0.1", -10, 10, NodeType.COMPUTE);
		tree.insertItem(node1);
		assertTrue(tree.getRoot().getScore() == 1);
		NodeInfo node2 = new NodeInfo("child2", "127.0.0.1", -10, 10, NodeType.COMPUTE);
		tree.insertItem(node2);
		assertTrue(tree.getRoot().getScore() == 2);
		NodeInfo node3 = new NodeInfo("child3", "127.0.0.1", -10, 10, NodeType.COMPUTE);
		tree.insertItem(node3);
		assertTrue(tree.getRoot().getScore() == 3);
	} 

	@Test
	public void testNoSplit() {
		NodeInfo node1 = new NodeInfo("child1", "127.0.0.1", -10, 10, NodeType.COMPUTE);
		tree.insertItem(node1);
		assertTrue(tree.getRoot().getScore() == 1);
		NodeInfo node2 = new NodeInfo("child2", "127.0.0.1", -10, 10, NodeType.COMPUTE);
		tree.insertItem(node2);
		assertTrue(tree.getRoot().getScore() == 2);
		NodeInfo node3 = new NodeInfo("child3", "127.0.0.1", -10, 10, NodeType.COMPUTE);
		tree.insertItem(node3);
		assertTrue(tree.getRoot().getScore() == 3);
		NodeInfo node4 = new NodeInfo("child4", "127.0.0.1", -10, 10, NodeType.COMPUTE);
		tree.insertItem(node4);
		assertTrue(tree.getRoot().getScore() == 4);
	} 
	
	@Test
	public void testSplitNW() {
		NodeInfo NWNode = new NodeInfo("NW", "127.0.0.1", 80, -80, NodeType.COMPUTE); 
		tree.availableNodes.add(NWNode);
		assertTrue(tree.availableNodes.size() == 1);
		
		NodeInfo node1 = new NodeInfo("child1", "127.0.0.1", 40, -10, NodeType.COMPUTE);
		tree.insertItem(node1);
		assertTrue(tree.getRoot().getScore() == 1);
		NodeInfo node2 = new NodeInfo("child2", "127.0.0.1", 67, -20, NodeType.COMPUTE);
		tree.insertItem(node2);
		assertTrue(tree.getRoot().getScore() == 2);
		NodeInfo node3 = new NodeInfo("child3", "127.0.0.1", -10, 10, NodeType.COMPUTE);
		tree.insertItem(node3);
		assertTrue(tree.getRoot().getScore() == 3);

		NodeInfo node4 = new NodeInfo("child4", "127.0.0.1", -10, -10, NodeType.COMPUTE);
		tree.insertItem(node4);
		
		assertTrue(tree.availableNodes.size() == 0);
		assertTrue(tree.getRoot().getNW().getId().equals("NW"));
		assertTrue(tree.getRoot().coveredItems.size() == 2);
		assertTrue(tree.getRoot().getScore() == 2);
		assertTrue(tree.getRoot().getNW().getScore() == 2);
	}
	
	@Test
	public void testInsertToNW() {
		NodeInfo NWNode = new NodeInfo("NW", "127.0.0.1", 80, -80, NodeType.COMPUTE); 
		tree.availableNodes.add(NWNode);
		assertTrue(tree.availableNodes.size() == 1);
		
		NodeInfo node1 = new NodeInfo("child1", "127.0.0.1", 40, -10, NodeType.COMPUTE);
		tree.insertItem(node1);
		assertTrue(tree.getRoot().getScore() == 1);
		NodeInfo node2 = new NodeInfo("child2", "127.0.0.1", 67, -20, NodeType.COMPUTE);
		tree.insertItem(node2);
		assertTrue(tree.getRoot().getScore() == 2);
		NodeInfo node3 = new NodeInfo("child3", "127.0.0.1", -10, 10, NodeType.COMPUTE);
		tree.insertItem(node3);
		assertTrue(tree.getRoot().getScore() == 3);

		NodeInfo node4 = new NodeInfo("child4", "127.0.0.1", -10, -10, NodeType.COMPUTE);
		tree.insertItem(node4);
		
		assertTrue(tree.availableNodes.size() == 0);
		assertTrue(tree.getRoot().getNW().getId().equals("NW"));
		assertTrue(tree.getRoot().coveredItems.size() == 2);
		assertTrue(tree.getRoot().getScore() == 2);
		assertTrue(tree.getRoot().getNW().getScore() == 2);
		
		NodeInfo node5 = new NodeInfo("child5", "127.0.0.1", 10, -10, NodeType.COMPUTE);
		tree.insertItem(node5);
		assertTrue(tree.getRoot().coveredItems.size() == 2);
		assertTrue(tree.getRoot().getScore() == 2);
		assertTrue(tree.getRoot().getNW().coveredItems.size() == 3);
		assertTrue(tree.getRoot().getNW().getScore() == 3);
	}
	
	@Test
	public void testInsertToParent() {
		NodeInfo NWNode = new NodeInfo("NW", "127.0.0.1", 80, -80, NodeType.COMPUTE); 
		tree.availableNodes.add(NWNode);
		assertTrue(tree.availableNodes.size() == 1);
		
		NodeInfo node1 = new NodeInfo("child1", "127.0.0.1", 40, -10, NodeType.COMPUTE);
		tree.insertItem(node1);
		assertTrue(tree.getRoot().getScore() == 1);
		NodeInfo node2 = new NodeInfo("child2", "127.0.0.1", 67, -20, NodeType.COMPUTE);
		tree.insertItem(node2);
		assertTrue(tree.getRoot().getScore() == 2);
		NodeInfo node3 = new NodeInfo("child3", "127.0.0.1", -10, 10, NodeType.COMPUTE);
		tree.insertItem(node3);
		assertTrue(tree.getRoot().getScore() == 3);

		NodeInfo node4 = new NodeInfo("child4", "127.0.0.1", -10, -10, NodeType.COMPUTE);
		tree.insertItem(node4);
		
		assertTrue(tree.availableNodes.size() == 0);
		assertTrue(tree.getRoot().getNW().getId().equals("NW"));
		assertTrue(tree.getRoot().coveredItems.size() == 2);
		assertTrue(tree.getRoot().getScore() == 2);
		assertTrue(tree.getRoot().getNW().getScore() == 2);
		
		NodeInfo node5 = new NodeInfo("child5", "127.0.0.1", -20, -10, NodeType.COMPUTE);
		tree.insertItem(node5);
		assertTrue(tree.getRoot().coveredItems.size() == 3);
		assertTrue(tree.getRoot().getScore() == 3);
		assertTrue(tree.getRoot().getNW().coveredItems.size() == 2);
		assertTrue(tree.getRoot().getNW().getScore() == 2);
	}
	
	@Test
	public void testSplitNE() {
		NodeInfo NENode = new NodeInfo("NE", "127.0.0.1", 80, 90, NodeType.COMPUTE); 
		tree.availableNodes.add(NENode);
		assertTrue(tree.availableNodes.size() == 1);
		
		NodeInfo node1 = new NodeInfo("child1", "127.0.0.1", 40, -10, NodeType.COMPUTE);
		tree.insertItem(node1);
		assertTrue(tree.getRoot().getScore() == 1);
		NodeInfo node2 = new NodeInfo("child2", "127.0.0.1", 67, -20, NodeType.COMPUTE);
		tree.insertItem(node2);
		assertTrue(tree.getRoot().getScore() == 2);
		NodeInfo node3 = new NodeInfo("child3", "127.0.0.1", -10, 10, NodeType.COMPUTE);
		tree.insertItem(node3);
		assertTrue(tree.getRoot().getScore() == 3);

		NodeInfo node4 = new NodeInfo("child4", "127.0.0.1", 50, 10, NodeType.COMPUTE);
		tree.insertItem(node4);
		
		assertTrue(tree.availableNodes.size() == 0);
		assertTrue(tree.getRoot().getNE().getId().equals("NE"));
		assertTrue(tree.getRoot().coveredItems.size() == 3);
		assertTrue(tree.getRoot().getScore() == 3);
		assertTrue(tree.getRoot().getNE().getScore() == 1);
	}
	
	@Test
	public void testSplitSW() {
		NodeInfo SWNode = new NodeInfo("SW", "127.0.0.1", -80, -90, NodeType.COMPUTE); 
		tree.availableNodes.add(SWNode);
		assertTrue(tree.availableNodes.size() == 1);
		
		NodeInfo node1 = new NodeInfo("child1", "127.0.0.1", -40, -10, NodeType.COMPUTE);
		tree.insertItem(node1);
		assertTrue(tree.getRoot().getScore() == 1);
		NodeInfo node2 = new NodeInfo("child2", "127.0.0.1", 67, -20, NodeType.COMPUTE);
		tree.insertItem(node2);
		assertTrue(tree.getRoot().getScore() == 2);
		NodeInfo node3 = new NodeInfo("child3", "127.0.0.1", -10, 10, NodeType.COMPUTE);
		tree.insertItem(node3);
		assertTrue(tree.getRoot().getScore() == 3);

		NodeInfo node4 = new NodeInfo("child4", "127.0.0.1", 50, 10, NodeType.COMPUTE);
		tree.insertItem(node4);
		
		assertTrue(tree.availableNodes.size() == 0);
		assertTrue(tree.getRoot().getSW().getId().equals("SW"));
		assertTrue(tree.getRoot().coveredItems.size() == 3);
		assertTrue(tree.getRoot().getScore() == 3);
		assertTrue(tree.getRoot().getSW().getScore() == 1);
		assertTrue(tree.getRoot().getSW().coveredItems.get(0).getId().equals("child1"));
	}
	
	@Test
	public void testSplitSE() {
		NodeInfo SENode = new NodeInfo("SE", "127.0.0.1", -80, 90, NodeType.COMPUTE); 
		tree.availableNodes.add(SENode);
		assertTrue(tree.availableNodes.size() == 1);
		
		NodeInfo node1 = new NodeInfo("child1", "127.0.0.1", -40, -10, NodeType.COMPUTE);
		tree.insertItem(node1);
		assertTrue(tree.getRoot().getScore() == 1);
		NodeInfo node2 = new NodeInfo("child2", "127.0.0.1", 67, -20, NodeType.COMPUTE);
		tree.insertItem(node2);
		assertTrue(tree.getRoot().getScore() == 2);
		NodeInfo node3 = new NodeInfo("child3", "127.0.0.1", -10, 10, NodeType.COMPUTE);
		tree.insertItem(node3);
		assertTrue(tree.getRoot().getScore() == 3);

		NodeInfo node4 = new NodeInfo("child4", "127.0.0.1", 50, 10, NodeType.COMPUTE);
		tree.insertItem(node4);
		
		assertTrue(tree.availableNodes.size() == 0);
		assertTrue(tree.getRoot().getSE().getId().equals("SE"));
		assertTrue(tree.getRoot().coveredItems.size() == 3);
		assertTrue(tree.getRoot().getScore() == 3);
		assertTrue(tree.getRoot().getSE().getScore() == 1);
		assertTrue(tree.getRoot().getSE().coveredItems.get(0).getId().equals("child3"));
	}
	
	
	@Test
	public void testRemove() {
		NodeInfo node1 = new NodeInfo("child1", "127.0.0.1", -10, 10, NodeType.COMPUTE);
		tree.insertItem(node1);
		NodeInfo node2 = new NodeInfo("child2", "127.0.0.1", -10, 10, NodeType.COMPUTE);
		tree.insertItem(node2);
		NodeInfo node3 = new NodeInfo("child3", "127.0.0.1", -10, 10, NodeType.COMPUTE);
		tree.insertItem(node3);
		
		tree.remove(node2);
		assertTrue(tree.getRoot().getScore() == 2);
		tree.remove(node1);
		assertTrue(tree.getRoot().getScore() == 1);
		tree.remove(node1);
		assertTrue(tree.getRoot().getScore() == 1);
	}
	
	
	
}
