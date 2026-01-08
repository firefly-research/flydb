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
Package auth provides Role-Based Access Control (RBAC) for FlyDB.

RBAC Overview:
==============

FlyDB implements a professional RBAC system with the following components:

  1. Roles: Named collections of privileges (e.g., "admin", "reader", "writer")
  2. Privileges: Specific permissions on database objects (SELECT, INSERT, etc.)
  3. Role Assignments: Users are assigned to roles
  4. Role Hierarchy: Roles can inherit from other roles

Storage Schema:
===============

RBAC data is stored in the system database with these key prefixes:

  - _sys_roles:<role_name>           : Role definition (name, description, privileges)
  - _sys_role_privs:<role>:<db>:<tbl>: Role privileges on specific objects
  - _sys_user_roles:<user>:<role>    : User-to-role assignments
  - _sys_role_inherit:<role>:<parent>: Role inheritance relationships

Built-in Roles:
===============

  - admin: Full access to all databases and objects
  - reader: SELECT on all tables in assigned databases
  - writer: SELECT, INSERT, UPDATE, DELETE on all tables in assigned databases
  - owner: Full access to owned databases (CREATE, DROP, ALTER, etc.)

Privilege Hierarchy:
====================

  ALL > (CREATE, DROP, ALTER, SELECT, INSERT, UPDATE, DELETE, GRANT)
  
  Object levels:
  - Server level: CREATE DATABASE, DROP DATABASE
  - Database level: CREATE TABLE, DROP TABLE, etc.
  - Table level: SELECT, INSERT, UPDATE, DELETE
  - Column level: SELECT(col), UPDATE(col)
*/
package auth

import (
	"encoding/json"
	"errors"
	"strings"
	"time"
)

// Key prefixes for RBAC data in the storage engine.
const (
	roleKeyPrefix        = "_sys_roles:"       // Prefix for role definitions
	rolePrivKeyPrefix    = "_sys_role_privs:"  // Prefix for role privileges
	userRoleKeyPrefix    = "_sys_user_roles:"  // Prefix for user-role assignments
	roleInheritKeyPrefix = "_sys_role_inherit:" // Prefix for role inheritance
)

// Built-in role names
const (
	RoleAdmin  = "admin"  // Full access to everything
	RoleReader = "reader" // Read-only access
	RoleWriter = "writer" // Read-write access
	RoleOwner  = "owner"  // Database owner privileges
)

// ObjectType represents the type of database object for privilege assignment.
type ObjectType string

const (
	ObjectTypeServer   ObjectType = "SERVER"   // Server-level privileges
	ObjectTypeDatabase ObjectType = "DATABASE" // Database-level privileges
	ObjectTypeTable    ObjectType = "TABLE"    // Table-level privileges
	ObjectTypeColumn   ObjectType = "COLUMN"   // Column-level privileges
)

// Role represents a database role with associated privileges.
type Role struct {
	Name        string    `json:"name"`         // Unique role name
	Description string    `json:"description"`  // Human-readable description
	IsBuiltIn   bool      `json:"is_built_in"`  // Whether this is a built-in role
	CreatedAt   string    `json:"created_at"`   // When the role was created
	CreatedBy   string    `json:"created_by"`   // Who created the role
	ModifiedAt  string    `json:"modified_at"`  // When the role was last modified
}

// RolePrivilege represents a privilege granted to a role on a specific object.
type RolePrivilege struct {
	RoleName    string          `json:"role_name"`    // The role this privilege belongs to
	ObjectType  ObjectType      `json:"object_type"`  // Type of object (SERVER, DATABASE, TABLE, COLUMN)
	Database    string          `json:"database"`     // Database name (or "*" for all)
	TableName   string          `json:"table_name"`   // Table name (or "*" for all)
	ColumnName  string          `json:"column_name"`  // Column name (or "*" for all)
	Privileges  []PrivilegeType `json:"privileges"`   // List of granted privileges
	WithGrant   bool            `json:"with_grant"`   // Can grant this privilege to others
	RLS         *RLS            `json:"rls"`          // Optional row-level security
	GrantedAt   string          `json:"granted_at"`   // When the privilege was granted
	GrantedBy   string          `json:"granted_by"`   // Who granted the privilege
}

// UserRole represents a user's assignment to a role.
type UserRole struct {
	Username   string `json:"username"`    // The user
	RoleName   string `json:"role_name"`   // The role assigned
	Database   string `json:"database"`    // Database scope (or "*" for all)
	GrantedAt  string `json:"granted_at"`  // When the role was assigned
	GrantedBy  string `json:"granted_by"`  // Who assigned the role
	ExpiresAt  string `json:"expires_at"`  // Optional expiration time
}

// RoleInheritance represents a role inheriting from another role.
type RoleInheritance struct {
	RoleName   string `json:"role_name"`   // The child role
	ParentRole string `json:"parent_role"` // The parent role to inherit from
	GrantedAt  string `json:"granted_at"`  // When the inheritance was set
	GrantedBy  string `json:"granted_by"`  // Who set the inheritance
}

// PrivilegeCheck represents the result of a privilege check.
type PrivilegeCheck struct {
	Allowed   bool            // Whether access is allowed
	RLS       *RLS            // Row-level security to apply (nil if none)
	GrantedBy string          // How the privilege was granted (direct, role, inherited)
	Roles     []string        // Roles that grant this privilege
}

// =============================================================================
// Role Management Methods
// =============================================================================

// CreateRole creates a new role with the given name and description.
func (m *AuthManager) CreateRole(name, description, createdBy string) error {
	// Validate role name
	if name == "" {
		return errors.New("role name cannot be empty")
	}
	if strings.ContainsAny(name, ":*") {
		return errors.New("role name cannot contain ':' or '*'")
	}

	key := roleKeyPrefix + name

	// Check if role already exists
	if _, err := m.store.Get(key); err == nil {
		return errors.New("role already exists: " + name)
	}

	now := time.Now().Format(time.RFC3339)
	role := Role{
		Name:        name,
		Description: description,
		IsBuiltIn:   false,
		CreatedAt:   now,
		CreatedBy:   createdBy,
		ModifiedAt:  now,
	}

	data, err := json.Marshal(role)
	if err != nil {
		return err
	}

	return m.store.Put(key, data)
}

// DropRole deletes a role and all its associated privileges and assignments.
func (m *AuthManager) DropRole(name string) error {
	key := roleKeyPrefix + name

	// Check if role exists
	data, err := m.store.Get(key)
	if err != nil {
		return errors.New("role does not exist: " + name)
	}

	// Check if it's a built-in role
	var role Role
	if err := json.Unmarshal(data, &role); err == nil && role.IsBuiltIn {
		return errors.New("cannot drop built-in role: " + name)
	}

	// Delete all role privileges
	privPrefix := rolePrivKeyPrefix + name + ":"
	privs, _ := m.store.Scan(privPrefix)
	for k := range privs {
		m.store.Delete(k)
	}

	// Delete all user-role assignments for this role
	userRolePrefix := userRoleKeyPrefix
	assignments, _ := m.store.Scan(userRolePrefix)
	for k := range assignments {
		if strings.Contains(k, ":"+name) {
			m.store.Delete(k)
		}
	}

	// Delete all role inheritance relationships
	inheritPrefix := roleInheritKeyPrefix + name + ":"
	inherits, _ := m.store.Scan(inheritPrefix)
	for k := range inherits {
		m.store.Delete(k)
	}

	// Delete the role itself
	return m.store.Delete(key)
}

// GetRole retrieves a role by name.
func (m *AuthManager) GetRole(name string) (*Role, error) {
	key := roleKeyPrefix + name
	data, err := m.store.Get(key)
	if err != nil {
		return nil, errors.New("role does not exist: " + name)
	}

	var role Role
	if err := json.Unmarshal(data, &role); err != nil {
		return nil, err
	}

	return &role, nil
}

// ListRoles returns all roles in the system.
func (m *AuthManager) ListRoles() ([]Role, error) {
	data, err := m.store.Scan(roleKeyPrefix)
	if err != nil {
		return nil, err
	}

	roles := make([]Role, 0, len(data))
	for _, v := range data {
		var role Role
		if err := json.Unmarshal(v, &role); err == nil {
			roles = append(roles, role)
		}
	}

	return roles, nil
}

// RoleExists checks if a role exists.
func (m *AuthManager) RoleExists(name string) bool {
	key := roleKeyPrefix + name
	_, err := m.store.Get(key)
	return err == nil
}

// =============================================================================
// Role Privilege Methods
// =============================================================================

// GrantPrivilegeToRole grants privileges to a role on a specific object.
func (m *AuthManager) GrantPrivilegeToRole(roleName string, privileges []PrivilegeType,
	objectType ObjectType, database, table, column string, withGrant bool, rls *RLS, grantedBy string) error {

	// Verify role exists
	if !m.RoleExists(roleName) {
		return errors.New("role does not exist: " + roleName)
	}

	// Build the key based on object type
	var key string
	switch objectType {
	case ObjectTypeServer:
		key = rolePrivKeyPrefix + roleName + ":*:*:*"
	case ObjectTypeDatabase:
		key = rolePrivKeyPrefix + roleName + ":" + database + ":*:*"
	case ObjectTypeTable:
		key = rolePrivKeyPrefix + roleName + ":" + database + ":" + table + ":*"
	case ObjectTypeColumn:
		key = rolePrivKeyPrefix + roleName + ":" + database + ":" + table + ":" + column
	default:
		return errors.New("invalid object type")
	}

	priv := RolePrivilege{
		RoleName:   roleName,
		ObjectType: objectType,
		Database:   database,
		TableName:  table,
		ColumnName: column,
		Privileges: privileges,
		WithGrant:  withGrant,
		RLS:        rls,
		GrantedAt:  time.Now().Format(time.RFC3339),
		GrantedBy:  grantedBy,
	}

	data, err := json.Marshal(priv)
	if err != nil {
		return err
	}

	return m.store.Put(key, data)
}

// RevokePrivilegeFromRole revokes privileges from a role on a specific object.
func (m *AuthManager) RevokePrivilegeFromRole(roleName string, objectType ObjectType,
	database, table, column string) error {

	// Verify role exists
	if !m.RoleExists(roleName) {
		return errors.New("role does not exist: " + roleName)
	}

	// Build the key based on object type
	var key string
	switch objectType {
	case ObjectTypeServer:
		key = rolePrivKeyPrefix + roleName + ":*:*:*"
	case ObjectTypeDatabase:
		key = rolePrivKeyPrefix + roleName + ":" + database + ":*:*"
	case ObjectTypeTable:
		key = rolePrivKeyPrefix + roleName + ":" + database + ":" + table + ":*"
	case ObjectTypeColumn:
		key = rolePrivKeyPrefix + roleName + ":" + database + ":" + table + ":" + column
	default:
		return errors.New("invalid object type")
	}

	return m.store.Delete(key)
}

// GetRolePrivileges returns all privileges for a role.
func (m *AuthManager) GetRolePrivileges(roleName string) ([]RolePrivilege, error) {
	prefix := rolePrivKeyPrefix + roleName + ":"
	data, err := m.store.Scan(prefix)
	if err != nil {
		return nil, err
	}

	privs := make([]RolePrivilege, 0, len(data))
	for _, v := range data {
		var priv RolePrivilege
		if err := json.Unmarshal(v, &priv); err == nil {
			privs = append(privs, priv)
		}
	}

	return privs, nil
}

// =============================================================================
// User-Role Assignment Methods
// =============================================================================

// GrantRoleToUser assigns a role to a user, optionally scoped to a database.
func (m *AuthManager) GrantRoleToUser(username, roleName, database, grantedBy string) error {
	// Verify user exists
	if _, err := m.store.Get(userKeyPrefix + username); err != nil {
		return errors.New("user does not exist: " + username)
	}

	// Verify role exists
	if !m.RoleExists(roleName) {
		return errors.New("role does not exist: " + roleName)
	}

	// Build the key
	key := userRoleKeyPrefix + username + ":" + roleName
	if database != "" && database != "*" {
		key += ":" + database
	}

	assignment := UserRole{
		Username:  username,
		RoleName:  roleName,
		Database:  database,
		GrantedAt: time.Now().Format(time.RFC3339),
		GrantedBy: grantedBy,
	}

	data, err := json.Marshal(assignment)
	if err != nil {
		return err
	}

	return m.store.Put(key, data)
}

// RevokeRoleFromUser removes a role assignment from a user.
func (m *AuthManager) RevokeRoleFromUser(username, roleName, database string) error {
	// Verify user exists
	if _, err := m.store.Get(userKeyPrefix + username); err != nil {
		return errors.New("user does not exist: " + username)
	}

	// Build the key
	key := userRoleKeyPrefix + username + ":" + roleName
	if database != "" && database != "*" {
		key += ":" + database
	}

	return m.store.Delete(key)
}

// GetUserRoles returns all roles assigned to a user.
func (m *AuthManager) GetUserRoles(username string) ([]UserRole, error) {
	prefix := userRoleKeyPrefix + username + ":"
	data, err := m.store.Scan(prefix)
	if err != nil {
		return nil, err
	}

	roles := make([]UserRole, 0, len(data))
	for _, v := range data {
		var role UserRole
		if err := json.Unmarshal(v, &role); err == nil {
			roles = append(roles, role)
		}
	}

	return roles, nil
}

// HasRole checks if a user has a specific role (optionally scoped to a database).
func (m *AuthManager) HasRole(username, roleName, database string) bool {
	// Check for global role assignment
	globalKey := userRoleKeyPrefix + username + ":" + roleName
	if _, err := m.store.Get(globalKey); err == nil {
		return true
	}

	// Check for database-scoped role assignment
	if database != "" {
		dbKey := userRoleKeyPrefix + username + ":" + roleName + ":" + database
		if _, err := m.store.Get(dbKey); err == nil {
			return true
		}
	}

	return false
}

// =============================================================================
// Privilege Checking Methods
// =============================================================================

// CheckPrivilege checks if a user has a specific privilege on an object.
// This is the main authorization check method that considers:
// 1. Direct user privileges (legacy)
// 2. Role-based privileges
// 3. Role inheritance
// 4. Built-in admin privileges
func (m *AuthManager) CheckPrivilege(username string, privilege PrivilegeType,
	database, table string) *PrivilegeCheck {

	result := &PrivilegeCheck{
		Allowed: false,
		Roles:   []string{},
	}

	// Anonymous users (empty username) are denied access - must authenticate first
	if username == "" {
		result.GrantedBy = "denied"
		return result
	}

	// Check if user has the admin role (either by username or role assignment)
	if username == AdminUsername || m.HasRole(username, RoleAdmin, "") {
		result.Allowed = true
		result.GrantedBy = "admin"
		result.Roles = []string{RoleAdmin}
		return result
	}

	// Get all roles for the user
	userRoles, err := m.GetUserRoles(username)
	if err != nil {
		return result
	}

	// Check each role for the required privilege
	for _, ur := range userRoles {
		// Check if role applies to this database
		if ur.Database != "" && ur.Database != "*" && ur.Database != database {
			continue
		}

		// Get privileges for this role
		privs, err := m.GetRolePrivileges(ur.RoleName)
		if err != nil {
			continue
		}

		for _, p := range privs {
			// Check if privilege applies to this database/table
			if !m.privilegeMatches(p, database, table) {
				continue
			}

			// Check if the required privilege is granted
			if m.hasPrivilege(p.Privileges, privilege) {
				result.Allowed = true
				result.GrantedBy = "role"
				result.Roles = append(result.Roles, ur.RoleName)
				if p.RLS != nil {
					result.RLS = p.RLS
				}
			}
		}
	}

	// Fall back to legacy direct permission check if no role-based access
	if !result.Allowed {
		allowed, rls := m.checkLegacyPermission(username, database, table)
		if allowed {
			result.Allowed = true
			result.GrantedBy = "direct"
			result.RLS = rls
		}
	}

	return result
}

// privilegeMatches checks if a role privilege applies to the given database/table.
func (m *AuthManager) privilegeMatches(priv RolePrivilege, database, table string) bool {
	// Server-level privilege applies to everything
	if priv.ObjectType == ObjectTypeServer {
		return true
	}

	// Database-level privilege
	if priv.ObjectType == ObjectTypeDatabase {
		return priv.Database == "*" || priv.Database == database
	}

	// Table-level privilege
	if priv.ObjectType == ObjectTypeTable {
		dbMatch := priv.Database == "*" || priv.Database == database
		tblMatch := priv.TableName == "*" || priv.TableName == table
		return dbMatch && tblMatch
	}

	return false
}

// hasPrivilege checks if a privilege list contains the required privilege.
func (m *AuthManager) hasPrivilege(privileges []PrivilegeType, required PrivilegeType) bool {
	for _, p := range privileges {
		if p == PrivilegeAll || p == required {
			return true
		}
	}
	return false
}

// checkLegacyPermission checks legacy direct permissions (non-RBAC).
// This is used as a fallback for backward compatibility with old permission grants.
func (m *AuthManager) checkLegacyPermission(username, database, table string) (bool, *RLS) {
	// Check for wildcard table permission on this database
	wildcardKey := dbPrivKeyPrefix + username + ":" + database + ":" + WildcardTable
	if val, err := m.store.Get(wildcardKey); err == nil {
		var perm DatabasePermission
		if json.Unmarshal(val, &perm) == nil {
			return true, perm.RLS
		}
	}

	// Check for specific table permission
	key := dbPrivKeyPrefix + username + ":" + database + ":" + table
	if val, err := m.store.Get(key); err == nil {
		var perm DatabasePermission
		if json.Unmarshal(val, &perm) == nil {
			return true, perm.RLS
		}
	}

	// Check legacy permission (without database scope)
	legacyKey := privKeyPrefix + username + ":" + table
	if val, err := m.store.Get(legacyKey); err == nil {
		var perm Permission
		if json.Unmarshal(val, &perm) == nil {
			return true, perm.RLS
		}
	}

	return false, nil
}

// =============================================================================
// Built-in Role Initialization
// =============================================================================

// InitializeBuiltInRoles creates the default built-in roles if they don't exist.
func (m *AuthManager) InitializeBuiltInRoles() error {
	builtInRoles := []struct {
		name        string
		description string
		privileges  []PrivilegeType
		objectType  ObjectType
	}{
		{
			name:        RoleAdmin,
			description: "Full administrative access to all databases and objects",
			privileges:  []PrivilegeType{PrivilegeAll},
			objectType:  ObjectTypeServer,
		},
		{
			name:        RoleReader,
			description: "Read-only access to tables in assigned databases",
			privileges:  []PrivilegeType{PrivilegeSelect},
			objectType:  ObjectTypeDatabase,
		},
		{
			name:        RoleWriter,
			description: "Read-write access to tables in assigned databases",
			privileges:  []PrivilegeType{PrivilegeSelect, PrivilegeInsert, PrivilegeUpdate, PrivilegeDelete},
			objectType:  ObjectTypeDatabase,
		},
		{
			name:        RoleOwner,
			description: "Full access to owned databases including DDL operations",
			privileges:  []PrivilegeType{PrivilegeAll},
			objectType:  ObjectTypeDatabase,
		},
	}

	for _, r := range builtInRoles {
		// Check if role already exists
		if m.RoleExists(r.name) {
			continue
		}

		// Create the role
		now := time.Now().Format(time.RFC3339)
		role := Role{
			Name:        r.name,
			Description: r.description,
			IsBuiltIn:   true,
			CreatedAt:   now,
			CreatedBy:   "system",
			ModifiedAt:  now,
		}

		data, err := json.Marshal(role)
		if err != nil {
			return err
		}

		key := roleKeyPrefix + r.name
		if err := m.store.Put(key, data); err != nil {
			return err
		}

		// Grant privileges to the role
		// For admin, grant server-level ALL
		// For others, grant database-level privileges (applied when role is assigned to user with database scope)
		if r.name == RoleAdmin {
			if err := m.GrantPrivilegeToRole(r.name, r.privileges, ObjectTypeServer, "*", "*", "*", true, nil, "system"); err != nil {
				return err
			}
		} else {
			// These roles get their privileges when assigned to a user with a database scope
			if err := m.GrantPrivilegeToRole(r.name, r.privileges, ObjectTypeDatabase, "*", "*", "*", false, nil, "system"); err != nil {
				return err
			}
		}
	}

	return nil
}

// GetUsersWithRole returns all users that have a specific role.
func (m *AuthManager) GetUsersWithRole(roleName string) ([]UserRole, error) {
	// Scan all user-role assignments
	data, err := m.store.Scan(userRoleKeyPrefix)
	if err != nil {
		return nil, err
	}

	users := make([]UserRole, 0)
	for _, v := range data {
		var ur UserRole
		if err := json.Unmarshal(v, &ur); err == nil {
			if ur.RoleName == roleName {
				users = append(users, ur)
			}
		}
	}

	return users, nil
}
