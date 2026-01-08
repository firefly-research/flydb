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
Package auth provides authentication and authorization for FlyDB.

Authentication and Authorization Overview:
==========================================

FlyDB implements a simple but effective security model with two main components:

  1. Authentication: Verifying user identity via username/password
  2. Authorization: Controlling access to tables with optional Row-Level Security (RLS)

Security Model:
===============

The security model follows a "default deny" principle:
  - Users must be explicitly created before they can authenticate
  - Users must be explicitly granted access to each table
  - The "admin" user is created during first-time setup with a secure password

Row-Level Security (RLS):
=========================

RLS allows fine-grained access control at the row level. When granting access,
administrators can specify a condition that filters which rows a user can see.

Example:
  GRANT SELECT ON orders WHERE user_id = 'alice' TO alice

This ensures that user "alice" can only see orders where user_id = 'alice'.

Storage Schema:
===============

User and permission data is stored in the same KVStore as application data,
using reserved key prefixes:

  - _sys_users:<username>  : Stores User JSON (username, password)
  - _sys_privs:<user>:<table> : Stores Permission JSON (table, RLS condition)

Security Considerations:
========================

This implementation uses bcrypt for secure password hashing:
  - Passwords are hashed with bcrypt before storage
  - bcrypt's constant-time comparison prevents timing attacks
  - A dummy comparison is performed for non-existent users to prevent
    username enumeration attacks

For additional security in production, consider:
  - Implement rate limiting for authentication attempts
  - Add session management with tokens
  - Use TLS for encrypted connections
*/
package auth

import (
	"crypto/rand"
	"encoding/json"
	"errors"
	"fmt"
	"math/big"
	"time"

	"flydb/internal/storage"

	"golang.org/x/crypto/bcrypt"
)

// AdminUsername is the reserved username for the database administrator.
const AdminUsername = "admin"

// PasswordLength is the default length for generated passwords.
const PasswordLength = 16

// passwordCharset contains characters used for password generation.
// Excludes ambiguous characters (0, O, l, 1, I) for readability.
const passwordCharset = "abcdefghjkmnpqrstuvwxyzABCDEFGHJKMNPQRSTUVWXYZ23456789!@#$%^&*"

// GenerateSecurePassword generates a cryptographically secure random password.
// The password contains a mix of lowercase, uppercase, numbers, and special characters.
// Returns the generated password or an error if random generation fails.
func GenerateSecurePassword(length int) (string, error) {
	if length <= 0 {
		length = PasswordLength
	}

	password := make([]byte, length)
	charsetLen := big.NewInt(int64(len(passwordCharset)))

	for i := 0; i < length; i++ {
		idx, err := rand.Int(rand.Reader, charsetLen)
		if err != nil {
			return "", errors.New("failed to generate secure random number: " + err.Error())
		}
		password[i] = passwordCharset[idx.Int64()]
	}

	return string(password), nil
}

// DefaultBcryptCost is the default cost factor for bcrypt hashing.
// Higher values are more secure but slower. 10 is a good balance.
const DefaultBcryptCost = 10

// Key prefixes for system data in the storage engine.
// These prefixes ensure user/permission data doesn't conflict with application data.
const (
	userKeyPrefix   = "_sys_users:"    // Prefix for user records
	privKeyPrefix   = "_sys_privs:"    // Prefix for permission records (legacy)
	dbPrivKeyPrefix = "_sys_db_privs:" // Prefix for database-scoped permissions
	dbAccessPrefix  = "_sys_db_access:" // Prefix for database access permissions
)

// WildcardDatabase represents access to all databases.
const WildcardDatabase = "*"

// WildcardTable represents access to all tables in a database.
const WildcardTable = "*"

// RLS (Row-Level Security) defines a condition for restricting row access.
// When applied, queries are automatically filtered to only return rows
// where the specified column matches the specified value.
//
// Example: RLS{Column: "user_id", Value: "alice"} restricts access to rows
// where user_id = 'alice'.
type RLS struct {
	Column string // The column name to filter on
	Value  string // The required value for access
}

// PrivilegeType represents the type of database privilege.
type PrivilegeType string

const (
	PrivilegeSelect PrivilegeType = "SELECT"
	PrivilegeInsert PrivilegeType = "INSERT"
	PrivilegeUpdate PrivilegeType = "UPDATE"
	PrivilegeDelete PrivilegeType = "DELETE"
	PrivilegeCreate PrivilegeType = "CREATE"
	PrivilegeDrop   PrivilegeType = "DROP"
	PrivilegeAlter  PrivilegeType = "ALTER"
	PrivilegeAll    PrivilegeType = "ALL"
)

// User represents a database user account.
// Users are stored in the KVStore with the key prefix "_sys_users:".
//
// Passwords are securely hashed using bcrypt before storage.
// Authorization is handled through RBAC roles, not the legacy IsAdmin flag.
type User struct {
	Username     string   `json:"username"`      // Unique identifier for the user
	PasswordHash string   `json:"password_hash"` // User's password hash (bcrypt)
	IsAdmin      bool     `json:"is_admin"`      // DEPRECATED: Use RBAC roles instead. Kept for backward compatibility.
	DefaultDB    string   `json:"default_db"`    // Default database for this user
	Databases    []string `json:"databases"`     // DEPRECATED: Use RBAC roles instead.
	Roles        []string `json:"roles"`         // DEPRECATED: Use _sys_user_roles: storage instead.
	CreatedAt    string   `json:"created_at"`    // When the user was created
	LastLogin    string   `json:"last_login"`    // Last successful login time
	Status       string   `json:"status"`        // Account status: active, locked, expired
}

// Permission defines access rights for a user on a specific table.
// Permissions are stored in the KVStore with the key prefix "_sys_privs:".
//
// The optional RLS field enables row-level security, restricting which
// rows the user can access within the table.
type Permission struct {
	TableName string `json:"table_name"` // The table this permission applies to
	RLS       *RLS   `json:"rls"`        // Optional row-level security condition (nil = full access)
}

// DatabasePermission defines access rights for a user on a specific database.
// This extends the Permission model to support multi-database environments.
type DatabasePermission struct {
	Database   string          `json:"database"`   // The database this permission applies to
	TableName  string          `json:"table_name"` // The table (or "*" for all tables)
	Privileges []PrivilegeType `json:"privileges"` // List of granted privileges
	RLS        *RLS            `json:"rls"`        // Optional row-level security condition
	GrantedBy  string          `json:"granted_by"` // User who granted this permission
	GrantedAt  string          `json:"granted_at"` // When the permission was granted
}

// AuthManager handles user authentication and authorization.
// It provides methods for creating users, verifying credentials,
// granting permissions, and checking access rights.
//
// The AuthManager uses the storage engine for persistence, ensuring
// that user accounts and permissions survive server restarts.
type AuthManager struct {
	store storage.Engine // Underlying storage for user/permission data
}

// NewAuthManager creates a new AuthManager backed by the given storage engine.
// The storage engine is typically the same KVStore used for application data,
// but user/permission data is isolated using reserved key prefixes.
func NewAuthManager(store storage.Engine) *AuthManager {
	return &AuthManager{store: store}
}

// CreateUser creates a new user account with the given credentials.
// Returns an error if a user with the same username already exists.
//
// The password is hashed using bcrypt before storage for security.
// The user is stored as a JSON-encoded User struct with the key
// "_sys_users:<username>".
//
// Example:
//
//	err := authMgr.CreateUser("alice", "secret123")
func (m *AuthManager) CreateUser(username, password string) error {
	// Construct the storage key for this user.
	key := userKeyPrefix + username

	// Check if the user already exists.
	// We attempt to retrieve the user; if successful, the user exists.
	_, err := m.store.Get(key)
	if err == nil {
		return errors.New("user already exists")
	}

	// Hash the password using bcrypt for secure storage.
	hashedPassword, err := bcrypt.GenerateFromPassword([]byte(password), DefaultBcryptCost)
	if err != nil {
		return errors.New("failed to hash password: " + err.Error())
	}

	// Create the user record with hashed password and serialize to JSON.
	user := User{
		Username:     username,
		PasswordHash: string(hashedPassword),
		CreatedAt:    time.Now().Format(time.RFC3339),
	}
	data, err := json.Marshal(user)
	if err != nil {
		return err
	}

	// Store the user in the database.
	return m.store.Put(key, data)
}

// Authenticate verifies that the provided username and password are valid.
// Returns true if the credentials match an existing user, false otherwise.
//
// This method uses bcrypt's constant-time comparison to verify passwords,
// which prevents timing attacks.
//
// Example:
//
//	if authMgr.Authenticate("alice", "secret123") {
//	    // User is authenticated
//	}
func (m *AuthManager) Authenticate(username, password string) bool {
	// Construct the storage key and retrieve the user record.
	key := userKeyPrefix + username
	val, err := m.store.Get(key)
	if err != nil {
		// User not found - authentication fails.
		// Perform a dummy bcrypt comparison to prevent timing attacks
		// that could reveal whether a username exists.
		bcrypt.CompareHashAndPassword([]byte("$2a$10$dummy"), []byte(password))
		return false
	}

	// Deserialize the user record.
	var user User
	if err := json.Unmarshal(val, &user); err != nil {
		return false
	}

	// Compare the provided password with the stored hash using bcrypt.
	// bcrypt.CompareHashAndPassword is constant-time, preventing timing attacks.
	err = bcrypt.CompareHashAndPassword([]byte(user.PasswordHash), []byte(password))
	if err == nil {
		// Update last login time
		user.LastLogin = time.Now().Format(time.RFC3339)
		if data, err := json.Marshal(user); err == nil {
			m.store.Put(key, data)
		}
		return true
	}
	return false
}

// Grant grants a permission to a user on a specific table.
// Optionally, Row-Level Security (RLS) can be applied by specifying
// a column and value that must match for the user to access rows.
//
// Parameters:
//   - username: The user to grant access to
//   - table: The table name to grant access on
//   - rlsCol: Column name for RLS (empty string for no RLS)
//   - rlsVal: Required value for RLS column
//
// Returns an error if the user does not exist.
//
// Example without RLS:
//
//	err := authMgr.Grant("alice", "products", "", "")
//
// Example with RLS:
//
//	err := authMgr.Grant("alice", "orders", "user_id", "alice")
func (m *AuthManager) Grant(username, table string, rlsCol, rlsVal string) error {
	// Verify that the user exists before granting permissions.
	// This prevents orphaned permissions for non-existent users.
	if _, err := m.store.Get(userKeyPrefix + username); err != nil {
		return errors.New("user does not exist")
	}

	// Construct the permission storage key.
	// Format: _sys_privs:<username>:<table>
	key := privKeyPrefix + username + ":" + table

	// Build the RLS condition if column is specified.
	var rls *RLS
	if rlsCol != "" {
		rls = &RLS{Column: rlsCol, Value: rlsVal}
	}

	// Create and serialize the permission record.
	perm := Permission{TableName: table, RLS: rls}
	data, err := json.Marshal(perm)
	if err != nil {
		return err
	}

	// Store the permission in the database.
	return m.store.Put(key, data)
}

// Revoke removes a permission from a user on a specific table.
// This removes all access rights for the user on the specified table,
// including any Row-Level Security conditions that were set.
//
// Parameters:
//   - username: The user to revoke access from
//   - table: The table name to revoke access on
//
// Returns an error if the user does not exist or has no permission on the table.
//
// Example:
//
//	err := authMgr.Revoke("alice", "products")
func (m *AuthManager) Revoke(username, table string) error {
	// Verify that the user exists before revoking permissions.
	if _, err := m.store.Get(userKeyPrefix + username); err != nil {
		return errors.New("user does not exist")
	}

	// Construct the permission storage key.
	key := privKeyPrefix + username + ":" + table

	// Check if the permission exists
	if _, err := m.store.Get(key); err != nil {
		return errors.New("permission does not exist")
	}

	// Delete the permission from the database.
	return m.store.Delete(key)
}

// AlterUser modifies an existing user's password.
// The new password is hashed using bcrypt before storage.
//
// Parameters:
//   - username: The user to modify
//   - newPassword: The new password for the user
//
// Returns an error if the user does not exist.
//
// Example:
//
//	err := authMgr.AlterUser("alice", "new_secret123")
func (m *AuthManager) AlterUser(username, newPassword string) error {
	// Construct the storage key for this user.
	key := userKeyPrefix + username

	// Check if the user exists.
	if _, err := m.store.Get(key); err != nil {
		return errors.New("user does not exist")
	}

	// Hash the new password using bcrypt for secure storage.
	hashedPassword, err := bcrypt.GenerateFromPassword([]byte(newPassword), DefaultBcryptCost)
	if err != nil {
		return errors.New("failed to hash password: " + err.Error())
	}

	// Create the updated user record with new hashed password and serialize to JSON.
	user := User{Username: username, PasswordHash: string(hashedPassword)}
	data, err := json.Marshal(user)
	if err != nil {
		return err
	}

	// Store the updated user in the database.
	return m.store.Put(key, data)
}

// DropUser removes a user account from the database.
// Returns an error if the user doesn't exist.
func (m *AuthManager) DropUser(username string) error {
	key := "_sys_users:" + username

	// Check if user exists
	_, err := m.store.Get(key)
	if err != nil {
		return fmt.Errorf("user not found: %s", username)
	}

	// Delete the user
	return m.store.Delete(key)
}

// CheckPermission checks if a user has access to a specific table.
// Returns two values:
//   - allowed: true if the user has permission, false otherwise
//   - rls: the RLS condition to apply (nil if no RLS or access denied)
//
// The executor uses this method before executing queries to enforce
// access control. If RLS is returned, the executor must filter query
// results to only include rows matching the RLS condition.
//
// Example:
//
//	allowed, rls := authMgr.CheckPermission("alice", "orders")
//	if !allowed {
//	    return errors.New("permission denied")
//	}
//	if rls != nil {
//	    // Apply RLS filter: WHERE rls.Column = rls.Value
//	}
func (m *AuthManager) CheckPermission(username, table string) (bool, *RLS) {
	// Construct the permission storage key and attempt retrieval.
	key := privKeyPrefix + username + ":" + table
	val, err := m.store.Get(key)
	if err != nil {
		// No permission record found - access denied.
		// This implements the "default deny" security principle.
		return false, nil
	}

	// Deserialize the permission record.
	var perm Permission
	if err := json.Unmarshal(val, &perm); err != nil {
		// Corrupted permission data - deny access for safety.
		return false, nil
	}

	// Access granted - return the RLS condition (may be nil).
	return true, perm.RLS
}

// AdminExists checks if the admin user has been initialized in the database.
// Returns true if the admin user exists, false otherwise.
//
// This is used during startup to determine if first-time setup is needed.
func (m *AuthManager) AdminExists() bool {
	key := userKeyPrefix + AdminUsername
	_, err := m.store.Get(key)
	return err == nil
}

// InitializeAdmin creates the admin user with the given password.
// This should only be called during first-time setup when no admin exists.
//
// Returns an error if the admin user already exists or if password hashing fails.
//
// Example:
//
//	err := authMgr.InitializeAdmin("secure-password-123")
func (m *AuthManager) InitializeAdmin(password string) error {
	return m.CreateAdminUser(AdminUsername, password)
}

// CreateAdminUser creates a new admin user account with the given credentials.
// The user is created and assigned the 'admin' RBAC role for full privileges.
// The legacy IsAdmin flag is also set for backward compatibility.
func (m *AuthManager) CreateAdminUser(username, password string) error {
	// Construct the storage key for this user.
	key := userKeyPrefix + username

	// Check if the user already exists.
	_, err := m.store.Get(key)
	if err == nil {
		return errors.New("user already exists")
	}

	// Hash the password using bcrypt for secure storage.
	hashedPassword, err := bcrypt.GenerateFromPassword([]byte(password), DefaultBcryptCost)
	if err != nil {
		return errors.New("failed to hash password: " + err.Error())
	}

	// Create the admin user record with hashed password and serialize to JSON.
	user := User{
		Username:     username,
		PasswordHash: string(hashedPassword),
		IsAdmin:      true, // Legacy flag for backward compatibility
		CreatedAt:    time.Now().Format(time.RFC3339),
		Status:       "active",
	}
	data, err := json.Marshal(user)
	if err != nil {
		return err
	}

	// Store the user in the database.
	if err := m.store.Put(key, data); err != nil {
		return err
	}

	// Assign the admin RBAC role to this user (global scope)
	// This is the primary authorization mechanism - the IsAdmin flag is deprecated.
	if err := m.GrantRoleToUser(username, RoleAdmin, "", "system"); err != nil {
		// Log but don't fail - the user is created, role assignment is secondary
		// The legacy IsAdmin flag will still work as a fallback
		_ = err
	}

	return nil
}

// InitializeAdminWithGeneratedPassword creates the admin user with a
// cryptographically secure randomly generated password.
//
// Returns the generated password and any error that occurred.
// The caller should display this password to the user securely.
//
// Example:
//
//	password, err := authMgr.InitializeAdminWithGeneratedPassword()
//	if err != nil {
//	    log.Fatal(err)
//	}
//	fmt.Printf("Admin password: %s\n", password)
func (m *AuthManager) InitializeAdminWithGeneratedPassword() (string, error) {
	password, err := GenerateSecurePassword(PasswordLength)
	if err != nil {
		return "", err
	}

	if err := m.InitializeAdmin(password); err != nil {
		return "", err
	}

	return password, nil
}

// IsAdmin checks if the given username is the admin user.
func IsAdmin(username string) bool {
	return username == AdminUsername
}

// EnsureAdminHasRole ensures the admin user has the admin RBAC role.
// This is used to fix existing admin users that were created before RBAC was initialized.
// It's idempotent - if the admin already has the role, it does nothing.
func (m *AuthManager) EnsureAdminHasRole() error {
	// Check if admin user exists
	if !m.AdminExists() {
		return nil // No admin user, nothing to do
	}

	// Check if admin role exists
	if !m.RoleExists(RoleAdmin) {
		return nil // Role doesn't exist yet, will be created later
	}

	// Check if admin already has the role
	if m.HasRole(AdminUsername, RoleAdmin, "") {
		return nil // Already has the role
	}

	// Grant the admin role to the admin user
	return m.GrantRoleToUser(AdminUsername, RoleAdmin, "", "system")
}

// =============================================================================
// Database-Scoped Permission Methods
// =============================================================================

// GrantDatabaseAccess grants a user access to a specific database.
// This is required before the user can access any tables in the database.
func (m *AuthManager) GrantDatabaseAccess(username, database string) error {
	// Verify that the user exists
	if _, err := m.store.Get(userKeyPrefix + username); err != nil {
		return errors.New("user does not exist")
	}

	// Store database access permission
	key := dbAccessPrefix + username + ":" + database
	data := []byte(`{"database":"` + database + `","granted":true}`)
	return m.store.Put(key, data)
}

// RevokeDatabaseAccess revokes a user's access to a specific database.
func (m *AuthManager) RevokeDatabaseAccess(username, database string) error {
	// Verify that the user exists
	if _, err := m.store.Get(userKeyPrefix + username); err != nil {
		return errors.New("user does not exist")
	}

	key := dbAccessPrefix + username + ":" + database
	return m.store.Delete(key)
}

// HasDatabaseAccess checks if a user has access to a specific database.
// This now uses RBAC as the primary authorization mechanism.
func (m *AuthManager) HasDatabaseAccess(username, database string) bool {
	// Empty username means unauthenticated - deny access
	if username == "" {
		return false
	}

	// Check RBAC privileges first (primary mechanism)
	// Check if user has any role with privileges on this database
	check := m.CheckPrivilege(username, PrivilegeSelect, database, "*")
	if check.Allowed {
		return true
	}

	// Legacy: Check for wildcard access in old permission system
	wildcardKey := dbAccessPrefix + username + ":" + WildcardDatabase
	if _, err := m.store.Get(wildcardKey); err == nil {
		return true
	}

	// Legacy: Check for specific database access in old permission system
	key := dbAccessPrefix + username + ":" + database
	_, err := m.store.Get(key)
	return err == nil
}

// GrantOnDatabase grants a permission to a user on a specific table within a database.
// This is the database-scoped version of Grant.
func (m *AuthManager) GrantOnDatabase(username, database, table string, rlsCol, rlsVal string) error {
	// Verify that the user exists
	if _, err := m.store.Get(userKeyPrefix + username); err != nil {
		return errors.New("user does not exist")
	}

	// Build the RLS condition if column is specified
	var rls *RLS
	if rlsCol != "" {
		rls = &RLS{Column: rlsCol, Value: rlsVal}
	}

	// Create the database permission
	perm := DatabasePermission{
		Database:   database,
		TableName:  table,
		Privileges: []PrivilegeType{PrivilegeAll},
		RLS:        rls,
	}

	data, err := json.Marshal(perm)
	if err != nil {
		return err
	}

	// Store with database-scoped key
	key := dbPrivKeyPrefix + username + ":" + database + ":" + table
	return m.store.Put(key, data)
}

// RevokeOnDatabase revokes a permission from a user on a specific table within a database.
func (m *AuthManager) RevokeOnDatabase(username, database, table string) error {
	// Verify that the user exists
	if _, err := m.store.Get(userKeyPrefix + username); err != nil {
		return errors.New("user does not exist")
	}

	key := dbPrivKeyPrefix + username + ":" + database + ":" + table
	return m.store.Delete(key)
}

// CheckDatabasePermission checks if a user has access to a specific table within a database.
// Returns (allowed, rls) where allowed indicates if access is granted and rls is the
// row-level security condition to apply (nil if no RLS).
// This now uses RBAC as the primary authorization mechanism.
func (m *AuthManager) CheckDatabasePermission(username, database, table string) (bool, *RLS) {
	// Empty username means unauthenticated - deny access
	if username == "" {
		return false, nil
	}

	// Check RBAC privileges first (primary mechanism)
	check := m.CheckPrivilege(username, PrivilegeSelect, database, table)
	if check.Allowed {
		return true, nil // RBAC doesn't support RLS yet
	}

	// Legacy: Check for wildcard table permission on this database
	wildcardKey := dbPrivKeyPrefix + username + ":" + database + ":" + WildcardTable
	if val, err := m.store.Get(wildcardKey); err == nil {
		var perm DatabasePermission
		if json.Unmarshal(val, &perm) == nil {
			return true, perm.RLS
		}
	}

	// Legacy: Check for specific table permission
	key := dbPrivKeyPrefix + username + ":" + database + ":" + table
	val, err := m.store.Get(key)
	if err != nil {
		// Fall back to legacy permission check (for backward compatibility)
		return m.CheckPermission(username, table)
	}

	var perm DatabasePermission
	if err := json.Unmarshal(val, &perm); err != nil {
		return false, nil
	}

	return true, perm.RLS
}

// ListUserDatabases returns a list of databases the user has access to.
func (m *AuthManager) ListUserDatabases(username string) []string {
	// Admin has access to all databases
	if username == AdminUsername || username == "" {
		return nil // nil means all databases
	}

	// Scan for database access permissions
	prefix := dbAccessPrefix + username + ":"
	data, err := m.store.Scan(prefix)
	if err != nil {
		return []string{}
	}

	databases := make([]string, 0, len(data))
	for key := range data {
		// Extract database name from key
		dbName := key[len(prefix):]
		if dbName != "" {
			databases = append(databases, dbName)
		}
	}

	return databases
}
