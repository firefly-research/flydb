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
Package sql contains the TriggerManager component for database triggers.

TriggerManager Overview:
========================

The TriggerManager handles the registration, storage, and execution of database
triggers. Triggers are automatic actions that execute in response to INSERT,
UPDATE, or DELETE operations on tables.

Trigger Execution Flow:
=======================

 1. A DML operation (INSERT, UPDATE, DELETE) is initiated
 2. BEFORE triggers are executed (if any)
 3. The DML operation is performed
 4. AFTER triggers are executed (if any)

Trigger Storage:
================

Triggers are stored in the KVStore with the key format:

	trigger:<table>:<name> → Trigger JSON

This allows efficient lookup of all triggers for a specific table.

Thread Safety:
==============

The TriggerManager uses a sync.RWMutex to provide thread-safe access
to the trigger registry. Multiple readers can access triggers concurrently,
but writes are exclusive.
*/
package sql

import (
	"encoding/json"
	"errors"
	"flydb/internal/storage"
	"fmt"
	"sync"

	ferrors "flydb/internal/errors"
)

const triggerKeyPrefix = "trigger:"

// TriggerManager manages database triggers.
// It provides methods for creating, dropping, and executing triggers.
type TriggerManager struct {
	// triggers is the in-memory cache of triggers.
	// Key: table name, Value: map of trigger name to Trigger
	triggers map[string]map[string]*Trigger

	// store is the underlying storage engine for persistence.
	store storage.Engine

	// mu protects concurrent access to the triggers map.
	mu sync.RWMutex
}

// NewTriggerManager creates a new TriggerManager with the given storage engine.
// It loads existing triggers from storage into memory.
func NewTriggerManager(store storage.Engine) *TriggerManager {
	tm := &TriggerManager{
		triggers: make(map[string]map[string]*Trigger),
		store:    store,
	}
	tm.loadTriggers()
	return tm
}

// loadTriggers loads all triggers from storage into memory.
func (tm *TriggerManager) loadTriggers() {
	tm.mu.Lock()
	defer tm.mu.Unlock()

	// Scan for all trigger keys
	data, err := tm.store.Scan(triggerKeyPrefix)
	if err != nil {
		return
	}

	for _, val := range data {
		var trigger Trigger
		if err := json.Unmarshal(val, &trigger); err != nil {
			continue
		}

		if tm.triggers[trigger.TableName] == nil {
			tm.triggers[trigger.TableName] = make(map[string]*Trigger)
		}
		tm.triggers[trigger.TableName][trigger.Name] = &trigger
	}
}

// CreateTrigger creates a new trigger and persists it to storage.
// Returns an error if a trigger with the same name already exists on the table.
func (tm *TriggerManager) CreateTrigger(trigger *Trigger) error {
	tm.mu.Lock()
	defer tm.mu.Unlock()

	// Check if trigger already exists
	if tm.triggers[trigger.TableName] != nil {
		if _, exists := tm.triggers[trigger.TableName][trigger.Name]; exists {
			return ferrors.TriggerAlreadyExists(trigger.Name, trigger.TableName)
		}
	}

	// Serialize and store the trigger
	data, err := json.Marshal(trigger)
	if err != nil {
		return ferrors.InternalError("failed to serialize trigger").WithCause(err)
	}

	key := triggerKeyPrefix + trigger.TableName + ":" + trigger.Name
	if err := tm.store.Put(key, data); err != nil {
		return ferrors.NewStorageError("failed to store trigger").WithCause(err)
	}

	// Add to in-memory cache
	if tm.triggers[trigger.TableName] == nil {
		tm.triggers[trigger.TableName] = make(map[string]*Trigger)
	}
	tm.triggers[trigger.TableName][trigger.Name] = trigger

	return nil
}

// DropTrigger removes a trigger from the table.
// Returns an error if the trigger does not exist.
func (tm *TriggerManager) DropTrigger(tableName, triggerName string) error {
	tm.mu.Lock()
	defer tm.mu.Unlock()

	// Check if trigger exists
	if tm.triggers[tableName] == nil {
		return ferrors.TriggerNotFound(triggerName, tableName)
	}
	if _, exists := tm.triggers[tableName][triggerName]; !exists {
		return ferrors.TriggerNotFound(triggerName, tableName)
	}

	// Remove from storage
	key := triggerKeyPrefix + tableName + ":" + triggerName
	if err := tm.store.Delete(key); err != nil {
		return ferrors.NewStorageError("failed to delete trigger").WithCause(err)
	}

	// Remove from in-memory cache
	delete(tm.triggers[tableName], triggerName)
	if len(tm.triggers[tableName]) == 0 {
		delete(tm.triggers, tableName)
	}

	return nil
}

// GetTriggers returns all triggers for a table with the specified timing and event.
// Returns an empty slice if no matching triggers exist.
func (tm *TriggerManager) GetTriggers(tableName string, timing TriggerTiming, event TriggerEvent) []*Trigger {
	tm.mu.RLock()
	defer tm.mu.RUnlock()

	var result []*Trigger
	if tm.triggers[tableName] == nil {
		return result
	}

	for _, trigger := range tm.triggers[tableName] {
		if trigger.Timing == timing && trigger.Event == event {
			result = append(result, trigger)
		}
	}

	return result
}

// GetAllTriggersForTable returns all triggers for a table.
func (tm *TriggerManager) GetAllTriggersForTable(tableName string) []*Trigger {
	tm.mu.RLock()
	defer tm.mu.RUnlock()

	var result []*Trigger
	if tm.triggers[tableName] == nil {
		return result
	}

	for _, trigger := range tm.triggers[tableName] {
		result = append(result, trigger)
	}

	return result
}

// TriggerExists checks if a trigger with the given name exists on the table.
func (tm *TriggerManager) TriggerExists(tableName, triggerName string) bool {
	tm.mu.RLock()
	defer tm.mu.RUnlock()

	if tm.triggers[tableName] == nil {
		return false
	}
	_, exists := tm.triggers[tableName][triggerName]
	return exists
}

// DropAllTriggersForTable removes all triggers for a table.
// This is called when a table is dropped.
func (tm *TriggerManager) DropAllTriggersForTable(tableName string) error {
	tm.mu.Lock()
	defer tm.mu.Unlock()

	if tm.triggers[tableName] == nil {
		return nil
	}

	var errs []error
	for triggerName := range tm.triggers[tableName] {
		key := triggerKeyPrefix + tableName + ":" + triggerName
		if err := tm.store.Delete(key); err != nil {
			errs = append(errs, ferrors.NewStorageError(fmt.Sprintf("failed to delete trigger %s", triggerName)).WithCause(err))
		}
	}

	delete(tm.triggers, tableName)

	if len(errs) > 0 {
		return errors.Join(errs...)
	}
	return nil
}
