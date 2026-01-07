/*
 * Copyright (c) 2026 Firefly Software Solutions Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

/*
B-Tree Index Implementation
============================

This file implements a B-Tree data structure for efficient key lookups
and range scans. The B-Tree provides O(log N) search, insert, and delete
operations, replacing the O(N) full table scans for indexed columns.

B-Tree Properties:
==================

  - Each node can have at most 2*t children (t = minimum degree)
  - Each node (except root) has at least t-1 keys
  - All leaves are at the same depth
  - Keys within a node are sorted

Usage:
======

	tree := storage.NewBTree(4) // minimum degree 4
	tree.Insert("key1", "rowKey1")
	tree.Insert("key2", "rowKey2")
	rowKey, found := tree.Search("key1")
*/
package storage

import (
	"sync"
)

// BTreeNode represents a node in the B-Tree.
type BTreeNode struct {
	keys     []string   // Keys stored in this node
	values   []string   // Values (row keys) corresponding to each key
	children []*BTreeNode // Child nodes (nil for leaf nodes)
	leaf     bool       // True if this is a leaf node
}

// BTree is a balanced tree structure for efficient key lookups.
// It provides O(log N) search, insert, and delete operations.
//
// Thread Safety: All methods are safe for concurrent use.
type BTree struct {
	root *BTreeNode // Root node of the tree
	t    int        // Minimum degree (defines the range for number of keys)
	mu   sync.RWMutex
}

// NewBTree creates a new B-Tree with the specified minimum degree.
// The minimum degree t determines the range of keys per node:
//   - Each node has at most 2*t - 1 keys
//   - Each node (except root) has at least t - 1 keys
//
// A typical value for t is 4-16 for in-memory trees.
func NewBTree(t int) *BTree {
	return &BTree{
		root: &BTreeNode{leaf: true},
		t:    t,
	}
}

// Search looks up a key in the B-Tree.
// Returns the associated value (row key) and true if found,
// or empty string and false if not found.
//
// Time complexity: O(log N)
func (bt *BTree) Search(key string) (string, bool) {
	bt.mu.RLock()
	defer bt.mu.RUnlock()
	return bt.searchNode(bt.root, key)
}

// searchNode recursively searches for a key starting from the given node.
func (bt *BTree) searchNode(node *BTreeNode, key string) (string, bool) {
	i := 0
	// Find the first key greater than or equal to the search key
	for i < len(node.keys) && key > node.keys[i] {
		i++
	}

	// Check if we found the key
	if i < len(node.keys) && node.keys[i] == key {
		return node.values[i], true
	}

	// If this is a leaf, the key is not in the tree
	if node.leaf {
		return "", false
	}

	// Recurse into the appropriate child
	return bt.searchNode(node.children[i], key)
}

// Insert adds a key-value pair to the B-Tree.
// If the key already exists, the value is updated.
//
// Time complexity: O(log N)
func (bt *BTree) Insert(key, value string) {
	bt.mu.Lock()
	defer bt.mu.Unlock()

	root := bt.root

	// If root is full, split it and create a new root
	if len(root.keys) == 2*bt.t-1 {
		newRoot := &BTreeNode{leaf: false}
		newRoot.children = append(newRoot.children, root)
		bt.splitChild(newRoot, 0)
		bt.root = newRoot
		bt.insertNonFull(newRoot, key, value)
	} else {
		bt.insertNonFull(root, key, value)
	}
}

// insertNonFull inserts a key into a node that is guaranteed to be non-full.
func (bt *BTree) insertNonFull(node *BTreeNode, key, value string) {
	i := len(node.keys) - 1

	if node.leaf {
		// Find the position to insert and check for existing key
		for i >= 0 && key < node.keys[i] {
			i--
		}
		// Check if key already exists
		if i >= 0 && node.keys[i] == key {
			node.values[i] = value // Update existing
			return
		}
		// Insert new key-value pair
		node.keys = append(node.keys, "")
		node.values = append(node.values, "")
		copy(node.keys[i+2:], node.keys[i+1:])
		copy(node.values[i+2:], node.values[i+1:])
		node.keys[i+1] = key
		node.values[i+1] = value
	} else {
		// Find the child to recurse into
		for i >= 0 && key < node.keys[i] {
			i--
		}
		i++
		// Split child if full
		if len(node.children[i].keys) == 2*bt.t-1 {
			bt.splitChild(node, i)
			if key > node.keys[i] {
				i++
			}
		}
		bt.insertNonFull(node.children[i], key, value)
	}
}

// splitChild splits the i-th child of node, which must be full.
func (bt *BTree) splitChild(node *BTreeNode, i int) {
	t := bt.t
	child := node.children[i]
	newNode := &BTreeNode{leaf: child.leaf}

	// Move the upper half of keys/values to the new node
	midKey := child.keys[t-1]
	midVal := child.values[t-1]

	newNode.keys = append(newNode.keys, child.keys[t:]...)
	newNode.values = append(newNode.values, child.values[t:]...)
	child.keys = child.keys[:t-1]
	child.values = child.values[:t-1]

	// Move children if not a leaf
	if !child.leaf {
		newNode.children = append(newNode.children, child.children[t:]...)
		child.children = child.children[:t]
	}

	// Insert the middle key into the parent
	node.keys = append(node.keys, "")
	node.values = append(node.values, "")
	copy(node.keys[i+1:], node.keys[i:])
	copy(node.values[i+1:], node.values[i:])
	node.keys[i] = midKey
	node.values[i] = midVal

	// Insert the new child
	node.children = append(node.children, nil)
	copy(node.children[i+2:], node.children[i+1:])
	node.children[i+1] = newNode
}

// Delete removes a key from the B-Tree.
// Returns true if the key was found and deleted, false otherwise.
//
// Time complexity: O(log N)
func (bt *BTree) Delete(key string) bool {
	bt.mu.Lock()
	defer bt.mu.Unlock()

	if bt.root == nil || len(bt.root.keys) == 0 {
		return false
	}

	deleted := bt.deleteFromNode(bt.root, key)

	// If root has no keys but has a child, make the child the new root
	if len(bt.root.keys) == 0 && !bt.root.leaf {
		bt.root = bt.root.children[0]
	}

	return deleted
}

// deleteFromNode removes a key from the subtree rooted at node.
func (bt *BTree) deleteFromNode(node *BTreeNode, key string) bool {
	i := 0
	for i < len(node.keys) && key > node.keys[i] {
		i++
	}

	// Case 1: Key is in this node
	if i < len(node.keys) && node.keys[i] == key {
		if node.leaf {
			// Simply remove from leaf
			node.keys = append(node.keys[:i], node.keys[i+1:]...)
			node.values = append(node.values[:i], node.values[i+1:]...)
			return true
		}
		// For internal nodes, replace with predecessor and delete predecessor
		predKey, predVal := bt.getPredecessor(node, i)
		node.keys[i] = predKey
		node.values[i] = predVal
		return bt.deleteFromNode(node.children[i], predKey)
	}

	// Case 2: Key is not in this node
	if node.leaf {
		return false
	}

	// Ensure child has enough keys before recursing
	if len(node.children[i].keys) < bt.t {
		bt.fillChild(node, i)
		// Recalculate i after potential merge
		if i > len(node.keys) {
			i--
		}
	}

	return bt.deleteFromNode(node.children[i], key)
}

// getPredecessor returns the predecessor key-value of keys[i].
func (bt *BTree) getPredecessor(node *BTreeNode, i int) (string, string) {
	curr := node.children[i]
	for !curr.leaf {
		curr = curr.children[len(curr.children)-1]
	}
	return curr.keys[len(curr.keys)-1], curr.values[len(curr.values)-1]
}

// fillChild ensures that node.children[i] has at least t keys.
func (bt *BTree) fillChild(node *BTreeNode, i int) {
	if i > 0 && len(node.children[i-1].keys) >= bt.t {
		bt.borrowFromPrev(node, i)
	} else if i < len(node.children)-1 && len(node.children[i+1].keys) >= bt.t {
		bt.borrowFromNext(node, i)
	} else {
		if i < len(node.children)-1 {
			bt.mergeChildren(node, i)
		} else {
			bt.mergeChildren(node, i-1)
		}
	}
}

// borrowFromPrev borrows a key from the previous sibling.
func (bt *BTree) borrowFromPrev(node *BTreeNode, i int) {
	child := node.children[i]
	sibling := node.children[i-1]

	// Shift keys in child to make room
	child.keys = append([]string{node.keys[i-1]}, child.keys...)
	child.values = append([]string{node.values[i-1]}, child.values...)

	// Move key from sibling to parent
	node.keys[i-1] = sibling.keys[len(sibling.keys)-1]
	node.values[i-1] = sibling.values[len(sibling.values)-1]
	sibling.keys = sibling.keys[:len(sibling.keys)-1]
	sibling.values = sibling.values[:len(sibling.values)-1]

	// Move child pointer if not leaf
	if !child.leaf {
		child.children = append([]*BTreeNode{sibling.children[len(sibling.children)-1]}, child.children...)
		sibling.children = sibling.children[:len(sibling.children)-1]
	}
}

// borrowFromNext borrows a key from the next sibling.
func (bt *BTree) borrowFromNext(node *BTreeNode, i int) {
	child := node.children[i]
	sibling := node.children[i+1]

	// Move key from parent to child
	child.keys = append(child.keys, node.keys[i])
	child.values = append(child.values, node.values[i])

	// Move key from sibling to parent
	node.keys[i] = sibling.keys[0]
	node.values[i] = sibling.values[0]
	sibling.keys = sibling.keys[1:]
	sibling.values = sibling.values[1:]

	// Move child pointer if not leaf
	if !child.leaf {
		child.children = append(child.children, sibling.children[0])
		sibling.children = sibling.children[1:]
	}
}

// mergeChildren merges children[i] and children[i+1].
func (bt *BTree) mergeChildren(node *BTreeNode, i int) {
	child := node.children[i]
	sibling := node.children[i+1]

	// Add parent key to child
	child.keys = append(child.keys, node.keys[i])
	child.values = append(child.values, node.values[i])

	// Add sibling keys to child
	child.keys = append(child.keys, sibling.keys...)
	child.values = append(child.values, sibling.values...)

	// Add sibling children if not leaf
	if !child.leaf {
		child.children = append(child.children, sibling.children...)
	}

	// Remove key and child pointer from parent
	node.keys = append(node.keys[:i], node.keys[i+1:]...)
	node.values = append(node.values[:i], node.values[i+1:]...)
	node.children = append(node.children[:i+1], node.children[i+2:]...)
}

// Range returns all key-value pairs where key is in [start, end].
// If start is empty, starts from the beginning.
// If end is empty, goes to the end.
//
// Time complexity: O(log N + K) where K is the number of keys in range.
func (bt *BTree) Range(start, end string) []struct{ Key, Value string } {
	bt.mu.RLock()
	defer bt.mu.RUnlock()

	var result []struct{ Key, Value string }
	bt.rangeNode(bt.root, start, end, &result)
	return result
}

// rangeNode collects keys in range from the subtree rooted at node.
func (bt *BTree) rangeNode(node *BTreeNode, start, end string, result *[]struct{ Key, Value string }) {
	if node == nil {
		return
	}

	i := 0
	// Find starting position
	if start != "" {
		for i < len(node.keys) && node.keys[i] < start {
			i++
		}
	}

	for i < len(node.keys) {
		// Check if we've passed the end
		if end != "" && node.keys[i] > end {
			return
		}

		// Visit left child first (if not leaf)
		if !node.leaf && i < len(node.children) {
			bt.rangeNode(node.children[i], start, end, result)
		}

		// Add current key if in range
		if (start == "" || node.keys[i] >= start) && (end == "" || node.keys[i] <= end) {
			*result = append(*result, struct{ Key, Value string }{node.keys[i], node.values[i]})
		}

		i++
	}

	// Visit rightmost child
	if !node.leaf && i < len(node.children) {
		bt.rangeNode(node.children[i], start, end, result)
	}
}

// Size returns the number of keys in the B-Tree.
func (bt *BTree) Size() int {
	bt.mu.RLock()
	defer bt.mu.RUnlock()
	return bt.sizeNode(bt.root)
}

func (bt *BTree) sizeNode(node *BTreeNode) int {
	if node == nil {
		return 0
	}
	count := len(node.keys)
	for _, child := range node.children {
		count += bt.sizeNode(child)
	}
	return count
}

