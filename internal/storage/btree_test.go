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

package storage

import (
	"fmt"
	"testing"
)

func TestBTreeInsertAndSearch(t *testing.T) {
	tree := NewBTree(4)

	// Insert some values
	tree.Insert("key1", "value1")
	tree.Insert("key2", "value2")
	tree.Insert("key3", "value3")

	// Search for existing keys
	val, found := tree.Search("key1")
	if !found || val != "value1" {
		t.Errorf("Expected value1, got %s (found=%v)", val, found)
	}

	val, found = tree.Search("key2")
	if !found || val != "value2" {
		t.Errorf("Expected value2, got %s (found=%v)", val, found)
	}

	// Search for non-existing key
	_, found = tree.Search("key999")
	if found {
		t.Error("Expected key999 to not be found")
	}
}

func TestBTreeUpdate(t *testing.T) {
	tree := NewBTree(4)

	tree.Insert("key1", "value1")
	tree.Insert("key1", "updated_value1")

	val, found := tree.Search("key1")
	if !found || val != "updated_value1" {
		t.Errorf("Expected updated_value1, got %s", val)
	}
}

func TestBTreeDelete(t *testing.T) {
	tree := NewBTree(4)

	tree.Insert("key1", "value1")
	tree.Insert("key2", "value2")
	tree.Insert("key3", "value3")

	// Delete key2
	deleted := tree.Delete("key2")
	if !deleted {
		t.Error("Expected key2 to be deleted")
	}

	// Verify key2 is gone
	_, found := tree.Search("key2")
	if found {
		t.Error("Expected key2 to not be found after deletion")
	}

	// Verify other keys still exist
	val, found := tree.Search("key1")
	if !found || val != "value1" {
		t.Error("Expected key1 to still exist")
	}

	val, found = tree.Search("key3")
	if !found || val != "value3" {
		t.Error("Expected key3 to still exist")
	}

	// Delete non-existing key
	deleted = tree.Delete("key999")
	if deleted {
		t.Error("Expected key999 deletion to return false")
	}
}

func TestBTreeRange(t *testing.T) {
	tree := NewBTree(4)

	// Insert keys in random order
	tree.Insert("c", "3")
	tree.Insert("a", "1")
	tree.Insert("e", "5")
	tree.Insert("b", "2")
	tree.Insert("d", "4")

	// Range query [b, d]
	results := tree.Range("b", "d")
	if len(results) != 3 {
		t.Errorf("Expected 3 results, got %d", len(results))
	}

	// Verify results are in order
	expected := []string{"b", "c", "d"}
	for i, r := range results {
		if r.Key != expected[i] {
			t.Errorf("Expected key %s at position %d, got %s", expected[i], i, r.Key)
		}
	}
}

func TestBTreeSize(t *testing.T) {
	tree := NewBTree(4)

	if tree.Size() != 0 {
		t.Errorf("Expected size 0, got %d", tree.Size())
	}

	tree.Insert("key1", "value1")
	tree.Insert("key2", "value2")
	tree.Insert("key3", "value3")

	if tree.Size() != 3 {
		t.Errorf("Expected size 3, got %d", tree.Size())
	}

	tree.Delete("key2")

	if tree.Size() != 2 {
		t.Errorf("Expected size 2, got %d", tree.Size())
	}
}

func TestBTreeManyInserts(t *testing.T) {
	tree := NewBTree(4)

	// Insert many values to trigger splits
	for i := 0; i < 100; i++ {
		key := fmt.Sprintf("key%03d", i)
		value := fmt.Sprintf("value%03d", i)
		tree.Insert(key, value)
	}

	// Verify all values can be found
	for i := 0; i < 100; i++ {
		key := fmt.Sprintf("key%03d", i)
		expectedValue := fmt.Sprintf("value%03d", i)
		val, found := tree.Search(key)
		if !found || val != expectedValue {
			t.Errorf("Expected %s for key %s, got %s (found=%v)", expectedValue, key, val, found)
		}
	}

	if tree.Size() != 100 {
		t.Errorf("Expected size 100, got %d", tree.Size())
	}
}

