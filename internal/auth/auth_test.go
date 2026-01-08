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

package auth

import (
	"encoding/json"
	"os"
	"strings"
	"testing"

	"flydb/internal/storage"
)

func setupTestAuthManager(t *testing.T) (*AuthManager, func()) {
	tmpDir, err := os.MkdirTemp("", "flydb_auth_test_*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}

	store, err := storage.NewKVStore(tmpDir + "/test.db")
	if err != nil {
		os.RemoveAll(tmpDir)
		t.Fatalf("Failed to create KVStore: %v", err)
	}

	authMgr := NewAuthManager(store)

	cleanup := func() {
		store.Close()
		os.RemoveAll(tmpDir)
	}

	return authMgr, cleanup
}

func TestCreateUser(t *testing.T) {
	authMgr, cleanup := setupTestAuthManager(t)
	defer cleanup()

	// Create a new user
	err := authMgr.CreateUser("alice", "secret123")
	if err != nil {
		t.Fatalf("CreateUser failed: %v", err)
	}

	// Try to create the same user again - should fail
	err = authMgr.CreateUser("alice", "different_password")
	if err == nil {
		t.Error("Expected error when creating duplicate user")
	}
}

func TestAuthenticate(t *testing.T) {
	authMgr, cleanup := setupTestAuthManager(t)
	defer cleanup()

	// Create a user
	err := authMgr.CreateUser("bob", "password123")
	if err != nil {
		t.Fatalf("CreateUser failed: %v", err)
	}

	// Test successful authentication
	if !authMgr.Authenticate("bob", "password123") {
		t.Error("Expected authentication to succeed with correct password")
	}

	// Test failed authentication with wrong password
	if authMgr.Authenticate("bob", "wrongpassword") {
		t.Error("Expected authentication to fail with wrong password")
	}

	// Test failed authentication with non-existent user
	if authMgr.Authenticate("nonexistent", "password") {
		t.Error("Expected authentication to fail for non-existent user")
	}
}

func TestGrantWithoutRLS(t *testing.T) {
	authMgr, cleanup := setupTestAuthManager(t)
	defer cleanup()

	// Create a user
	err := authMgr.CreateUser("charlie", "pass")
	if err != nil {
		t.Fatalf("CreateUser failed: %v", err)
	}

	// Grant permission without RLS
	err = authMgr.Grant("charlie", "products", "", "")
	if err != nil {
		t.Fatalf("Grant failed: %v", err)
	}

	// Check permission
	allowed, rls := authMgr.CheckPermission("charlie", "products")
	if !allowed {
		t.Error("Expected permission to be granted")
	}
	if rls != nil {
		t.Error("Expected no RLS condition")
	}
}

func TestGrantWithRLS(t *testing.T) {
	authMgr, cleanup := setupTestAuthManager(t)
	defer cleanup()

	// Create a user
	err := authMgr.CreateUser("diana", "pass")
	if err != nil {
		t.Fatalf("CreateUser failed: %v", err)
	}

	// Grant permission with RLS
	err = authMgr.Grant("diana", "orders", "user_id", "diana")
	if err != nil {
		t.Fatalf("Grant failed: %v", err)
	}

	// Check permission
	allowed, rls := authMgr.CheckPermission("diana", "orders")
	if !allowed {
		t.Error("Expected permission to be granted")
	}
	if rls == nil {
		t.Fatal("Expected RLS condition")
	}
	if rls.Column != "user_id" || rls.Value != "diana" {
		t.Errorf("Expected RLS user_id=diana, got %s=%s", rls.Column, rls.Value)
	}
}

func TestGrantToNonExistentUser(t *testing.T) {
	authMgr, cleanup := setupTestAuthManager(t)
	defer cleanup()

	// Try to grant permission to non-existent user
	err := authMgr.Grant("nonexistent", "products", "", "")
	if err == nil {
		t.Error("Expected error when granting to non-existent user")
	}
}

func TestCheckPermissionDenied(t *testing.T) {
	authMgr, cleanup := setupTestAuthManager(t)
	defer cleanup()

	// Create a user but don't grant any permissions
	err := authMgr.CreateUser("eve", "pass")
	if err != nil {
		t.Fatalf("CreateUser failed: %v", err)
	}

	// Check permission - should be denied
	allowed, _ := authMgr.CheckPermission("eve", "secret_table")
	if allowed {
		t.Error("Expected permission to be denied")
	}
}

func TestMultiplePermissions(t *testing.T) {
	authMgr, cleanup := setupTestAuthManager(t)
	defer cleanup()

	// Create a user
	err := authMgr.CreateUser("frank", "pass")
	if err != nil {
		t.Fatalf("CreateUser failed: %v", err)
	}

	// Grant permissions on multiple tables
	err = authMgr.Grant("frank", "table1", "", "")
	if err != nil {
		t.Fatalf("Grant failed: %v", err)
	}
	err = authMgr.Grant("frank", "table2", "owner", "frank")
	if err != nil {
		t.Fatalf("Grant failed: %v", err)
	}

	// Check table1 - no RLS
	allowed, rls := authMgr.CheckPermission("frank", "table1")
	if !allowed || rls != nil {
		t.Error("Expected table1 access without RLS")
	}

	// Check table2 - with RLS
	allowed, rls = authMgr.CheckPermission("frank", "table2")
	if !allowed || rls == nil {
		t.Error("Expected table2 access with RLS")
	}

	// Check table3 - no permission
	allowed, _ = authMgr.CheckPermission("frank", "table3")
	if allowed {
		t.Error("Expected table3 access to be denied")
	}
}

func TestPasswordIsHashed(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "flydb_auth_test_*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	store, err := storage.NewKVStore(tmpDir + "/test.db")
	if err != nil {
		t.Fatalf("Failed to create KVStore: %v", err)
	}
	defer store.Close()

	authMgr := NewAuthManager(store)

	// Create a user with a known password
	password := "mysecretpassword"
	err = authMgr.CreateUser("hashtest", password)
	if err != nil {
		t.Fatalf("CreateUser failed: %v", err)
	}

	// Retrieve the stored user data directly from storage
	data, err := store.Get("_sys_users:hashtest")
	if err != nil {
		t.Fatalf("Failed to get user data: %v", err)
	}

	var user User
	if err := json.Unmarshal(data, &user); err != nil {
		t.Fatalf("Failed to unmarshal user: %v", err)
	}

	// Verify password is NOT stored in plaintext
	if user.PasswordHash == password {
		t.Error("Password is stored in plaintext - should be hashed!")
	}

	// Verify password hash starts with bcrypt prefix ($2a$)
	if !strings.HasPrefix(user.PasswordHash, "$2a$") {
		t.Errorf("Password hash doesn't look like bcrypt: %s", user.PasswordHash)
	}

	// Verify authentication still works
	if !authMgr.Authenticate("hashtest", password) {
		t.Error("Authentication failed with correct password")
	}
}

func TestAuthenticateTimingAttackPrevention(t *testing.T) {
	authMgr, cleanup := setupTestAuthManager(t)
	defer cleanup()

	// Create a user
	err := authMgr.CreateUser("timingtest", "password123")
	if err != nil {
		t.Fatalf("CreateUser failed: %v", err)
	}

	// Both of these should take similar time due to dummy bcrypt comparison
	// for non-existent users (we can't easily test timing, but we verify
	// the code path doesn't panic)
	authMgr.Authenticate("timingtest", "wrongpassword")
	authMgr.Authenticate("nonexistent_user", "anypassword")

	// Just verify no panic occurred - timing attack prevention is in place
	t.Log("Timing attack prevention code paths executed successfully")
}

func TestAlterUser(t *testing.T) {
	authMgr, cleanup := setupTestAuthManager(t)
	defer cleanup()

	// Create a user
	err := authMgr.CreateUser("altertest", "oldpassword")
	if err != nil {
		t.Fatalf("CreateUser failed: %v", err)
	}

	// Verify old password works
	if !authMgr.Authenticate("altertest", "oldpassword") {
		t.Error("Expected authentication to succeed with old password")
	}

	// Alter the user's password
	err = authMgr.AlterUser("altertest", "newpassword")
	if err != nil {
		t.Fatalf("AlterUser failed: %v", err)
	}

	// Verify old password no longer works
	if authMgr.Authenticate("altertest", "oldpassword") {
		t.Error("Expected authentication to fail with old password after ALTER USER")
	}

	// Verify new password works
	if !authMgr.Authenticate("altertest", "newpassword") {
		t.Error("Expected authentication to succeed with new password")
	}
}

func TestAlterUserNonExistent(t *testing.T) {
	authMgr, cleanup := setupTestAuthManager(t)
	defer cleanup()

	// Try to alter a non-existent user
	err := authMgr.AlterUser("nonexistent", "newpassword")
	if err == nil {
		t.Error("Expected error when altering non-existent user")
	}
	if err.Error() != "user does not exist" {
		t.Errorf("Expected 'user does not exist' error, got: %v", err)
	}
}

func TestRevoke(t *testing.T) {
	authMgr, cleanup := setupTestAuthManager(t)
	defer cleanup()

	// Create a user
	err := authMgr.CreateUser("revoketest", "pass")
	if err != nil {
		t.Fatalf("CreateUser failed: %v", err)
	}

	// Grant permission
	err = authMgr.Grant("revoketest", "products", "", "")
	if err != nil {
		t.Fatalf("Grant failed: %v", err)
	}

	// Verify permission exists
	allowed, _ := authMgr.CheckPermission("revoketest", "products")
	if !allowed {
		t.Error("Expected permission to be granted")
	}

	// Revoke permission
	err = authMgr.Revoke("revoketest", "products")
	if err != nil {
		t.Fatalf("Revoke failed: %v", err)
	}

	// Verify permission is revoked
	allowed, _ = authMgr.CheckPermission("revoketest", "products")
	if allowed {
		t.Error("Expected permission to be revoked")
	}
}

func TestRevokeWithRLS(t *testing.T) {
	authMgr, cleanup := setupTestAuthManager(t)
	defer cleanup()

	// Create a user
	err := authMgr.CreateUser("revokerls", "pass")
	if err != nil {
		t.Fatalf("CreateUser failed: %v", err)
	}

	// Grant permission with RLS
	err = authMgr.Grant("revokerls", "orders", "user_id", "revokerls")
	if err != nil {
		t.Fatalf("Grant failed: %v", err)
	}

	// Verify permission exists with RLS
	allowed, rls := authMgr.CheckPermission("revokerls", "orders")
	if !allowed || rls == nil {
		t.Error("Expected permission with RLS to be granted")
	}

	// Revoke permission
	err = authMgr.Revoke("revokerls", "orders")
	if err != nil {
		t.Fatalf("Revoke failed: %v", err)
	}

	// Verify permission is revoked
	allowed, _ = authMgr.CheckPermission("revokerls", "orders")
	if allowed {
		t.Error("Expected permission to be revoked")
	}
}

func TestRevokeNonExistentUser(t *testing.T) {
	authMgr, cleanup := setupTestAuthManager(t)
	defer cleanup()

	// Try to revoke from non-existent user
	err := authMgr.Revoke("nonexistent", "products")
	if err == nil {
		t.Error("Expected error when revoking from non-existent user")
	}
	if err.Error() != "user does not exist" {
		t.Errorf("Expected 'user does not exist' error, got: %v", err)
	}
}

func TestRevokeNonExistentPermission(t *testing.T) {
	authMgr, cleanup := setupTestAuthManager(t)
	defer cleanup()

	// Create a user but don't grant any permissions
	err := authMgr.CreateUser("revokenoperm", "pass")
	if err != nil {
		t.Fatalf("CreateUser failed: %v", err)
	}

	// Try to revoke a permission that doesn't exist
	err = authMgr.Revoke("revokenoperm", "products")
	if err == nil {
		t.Error("Expected error when revoking non-existent permission")
	}
	if err.Error() != "permission does not exist" {
		t.Errorf("Expected 'permission does not exist' error, got: %v", err)
	}
}

// =============================================================================
// RBAC Tests
// =============================================================================

func TestCreateRole(t *testing.T) {
	authMgr, cleanup := setupTestAuthManager(t)
	defer cleanup()

	// Create a new role
	err := authMgr.CreateRole("analyst", "Data analyst role", "admin")
	if err != nil {
		t.Fatalf("CreateRole failed: %v", err)
	}

	// Verify role exists
	role, err := authMgr.GetRole("analyst")
	if err != nil {
		t.Fatalf("GetRole failed: %v", err)
	}
	if role.Name != "analyst" {
		t.Errorf("Expected role name 'analyst', got '%s'", role.Name)
	}
	if role.Description != "Data analyst role" {
		t.Errorf("Expected description 'Data analyst role', got '%s'", role.Description)
	}
	if role.CreatedBy != "admin" {
		t.Errorf("Expected created_by 'admin', got '%s'", role.CreatedBy)
	}

	// Try to create the same role again - should fail
	err = authMgr.CreateRole("analyst", "Another description", "admin")
	if err == nil {
		t.Error("Expected error when creating duplicate role")
	}
}

func TestDropRole(t *testing.T) {
	authMgr, cleanup := setupTestAuthManager(t)
	defer cleanup()

	// Create a role
	err := authMgr.CreateRole("temp_role", "Temporary role", "admin")
	if err != nil {
		t.Fatalf("CreateRole failed: %v", err)
	}

	// Drop the role
	err = authMgr.DropRole("temp_role")
	if err != nil {
		t.Fatalf("DropRole failed: %v", err)
	}

	// Verify role no longer exists
	if authMgr.RoleExists("temp_role") {
		t.Error("Role should not exist after drop")
	}

	// Try to drop non-existent role
	err = authMgr.DropRole("nonexistent")
	if err == nil {
		t.Error("Expected error when dropping non-existent role")
	}
}

func TestListRoles(t *testing.T) {
	authMgr, cleanup := setupTestAuthManager(t)
	defer cleanup()

	// Create some roles
	authMgr.CreateRole("role1", "First role", "admin")
	authMgr.CreateRole("role2", "Second role", "admin")

	roles, err := authMgr.ListRoles()
	if err != nil {
		t.Fatalf("ListRoles failed: %v", err)
	}

	if len(roles) < 2 {
		t.Errorf("Expected at least 2 roles, got %d", len(roles))
	}

	// Check that our roles are in the list
	foundRole1, foundRole2 := false, false
	for _, r := range roles {
		if r.Name == "role1" {
			foundRole1 = true
		}
		if r.Name == "role2" {
			foundRole2 = true
		}
	}
	if !foundRole1 || !foundRole2 {
		t.Error("Expected to find both role1 and role2 in list")
	}
}

func TestGrantRoleToUser(t *testing.T) {
	authMgr, cleanup := setupTestAuthManager(t)
	defer cleanup()

	// Create a user and a role
	err := authMgr.CreateUser("bob", "password123")
	if err != nil {
		t.Fatalf("CreateUser failed: %v", err)
	}

	err = authMgr.CreateRole("developer", "Developer role", "admin")
	if err != nil {
		t.Fatalf("CreateRole failed: %v", err)
	}

	// Grant role to user
	err = authMgr.GrantRoleToUser("bob", "developer", "", "admin")
	if err != nil {
		t.Fatalf("GrantRoleToUser failed: %v", err)
	}

	// Verify user has the role
	if !authMgr.HasRole("bob", "developer", "") {
		t.Error("User should have the developer role")
	}

	// Get user roles
	roles, err := authMgr.GetUserRoles("bob")
	if err != nil {
		t.Fatalf("GetUserRoles failed: %v", err)
	}
	if len(roles) != 1 {
		t.Errorf("Expected 1 role, got %d", len(roles))
	}
	if roles[0].RoleName != "developer" {
		t.Errorf("Expected role 'developer', got '%s'", roles[0].RoleName)
	}
}

func TestRevokeRoleFromUser(t *testing.T) {
	authMgr, cleanup := setupTestAuthManager(t)
	defer cleanup()

	// Create a user and a role
	authMgr.CreateUser("charlie", "password123")
	authMgr.CreateRole("tester", "Tester role", "admin")
	authMgr.GrantRoleToUser("charlie", "tester", "", "admin")

	// Verify role is assigned
	if !authMgr.HasRole("charlie", "tester", "") {
		t.Fatal("User should have the tester role before revoke")
	}

	// Revoke the role
	err := authMgr.RevokeRoleFromUser("charlie", "tester", "")
	if err != nil {
		t.Fatalf("RevokeRoleFromUser failed: %v", err)
	}

	// Verify role is no longer assigned
	if authMgr.HasRole("charlie", "tester", "") {
		t.Error("User should not have the tester role after revoke")
	}
}

func TestGrantPrivilegeToRole(t *testing.T) {
	authMgr, cleanup := setupTestAuthManager(t)
	defer cleanup()

	// Create a role
	err := authMgr.CreateRole("reader", "Read-only role", "admin")
	if err != nil {
		t.Fatalf("CreateRole failed: %v", err)
	}

	// Grant SELECT privilege to the role
	err = authMgr.GrantPrivilegeToRole("reader", []PrivilegeType{PrivilegeSelect},
		ObjectTypeTable, "*", "*", "*", false, nil, "admin")
	if err != nil {
		t.Fatalf("GrantPrivilegeToRole failed: %v", err)
	}

	// Get role privileges
	privs, err := authMgr.GetRolePrivileges("reader")
	if err != nil {
		t.Fatalf("GetRolePrivileges failed: %v", err)
	}
	if len(privs) != 1 {
		t.Errorf("Expected 1 privilege, got %d", len(privs))
	}
	if len(privs[0].Privileges) != 1 || privs[0].Privileges[0] != PrivilegeSelect {
		t.Error("Expected SELECT privilege")
	}
}

func TestCheckPrivilege(t *testing.T) {
	authMgr, cleanup := setupTestAuthManager(t)
	defer cleanup()

	// Create user, role, and grant privileges
	authMgr.CreateUser("dave", "password123")
	authMgr.CreateRole("data_reader", "Data reader role", "admin")
	authMgr.GrantPrivilegeToRole("data_reader", []PrivilegeType{PrivilegeSelect},
		ObjectTypeTable, "*", "users", "*", false, nil, "admin")
	authMgr.GrantRoleToUser("dave", "data_reader", "", "admin")

	// Check privilege - should be allowed
	check := authMgr.CheckPrivilege("dave", PrivilegeSelect, "default", "users")
	if !check.Allowed {
		t.Error("User should have SELECT privilege on users table")
	}
	if check.GrantedBy != "role" {
		t.Errorf("Expected granted_by 'role', got '%s'", check.GrantedBy)
	}

	// Check privilege on different table - should be denied (unless wildcard)
	check = authMgr.CheckPrivilege("dave", PrivilegeInsert, "default", "users")
	if check.Allowed {
		t.Error("User should not have INSERT privilege")
	}
}

func TestAdminHasAllPrivileges(t *testing.T) {
	authMgr, cleanup := setupTestAuthManager(t)
	defer cleanup()

	// Admin should have all privileges
	check := authMgr.CheckPrivilege("admin", PrivilegeAll, "any_db", "any_table")
	if !check.Allowed {
		t.Error("Admin should have all privileges")
	}
	if check.GrantedBy != "admin" {
		t.Errorf("Expected granted_by 'admin', got '%s'", check.GrantedBy)
	}
}

func TestInitializeBuiltInRoles(t *testing.T) {
	authMgr, cleanup := setupTestAuthManager(t)
	defer cleanup()

	// Initialize built-in roles
	err := authMgr.InitializeBuiltInRoles()
	if err != nil {
		t.Fatalf("InitializeBuiltInRoles failed: %v", err)
	}

	// Verify built-in roles exist
	builtInRoles := []string{RoleAdmin, RoleReader, RoleWriter, RoleOwner}
	for _, roleName := range builtInRoles {
		if !authMgr.RoleExists(roleName) {
			t.Errorf("Built-in role '%s' should exist", roleName)
		}
		role, err := authMgr.GetRole(roleName)
		if err != nil {
			t.Errorf("Failed to get built-in role '%s': %v", roleName, err)
		}
		if !role.IsBuiltIn {
			t.Errorf("Role '%s' should be marked as built-in", roleName)
		}
	}

	// Try to drop a built-in role - should fail
	err = authMgr.DropRole(RoleAdmin)
	if err == nil {
		t.Error("Should not be able to drop built-in role")
	}
}

func TestDropUser(t *testing.T) {
	authMgr, cleanup := setupTestAuthManager(t)
	defer cleanup()

	// Create a user
	err := authMgr.CreateUser("temp_user", "password123")
	if err != nil {
		t.Fatalf("CreateUser failed: %v", err)
	}

	// Verify user exists
	if !authMgr.Authenticate("temp_user", "password123") {
		t.Fatal("User should exist and authenticate")
	}

	// Drop the user
	err = authMgr.DropUser("temp_user")
	if err != nil {
		t.Fatalf("DropUser failed: %v", err)
	}

	// Verify user no longer exists
	if authMgr.Authenticate("temp_user", "password123") {
		t.Error("User should not exist after drop")
	}

	// Try to drop non-existent user
	err = authMgr.DropUser("nonexistent")
	if err == nil {
		t.Error("Expected error when dropping non-existent user")
	}
}

func TestGetUsersWithRole(t *testing.T) {
	authMgr, cleanup := setupTestAuthManager(t)
	defer cleanup()

	// Create users and a role
	authMgr.CreateUser("user1", "pass1")
	authMgr.CreateUser("user2", "pass2")
	authMgr.CreateUser("user3", "pass3")
	authMgr.CreateRole("shared_role", "Shared role", "admin")

	// Assign role to some users
	authMgr.GrantRoleToUser("user1", "shared_role", "", "admin")
	authMgr.GrantRoleToUser("user2", "shared_role", "", "admin")

	// Get users with the role
	users, err := authMgr.GetUsersWithRole("shared_role")
	if err != nil {
		t.Fatalf("GetUsersWithRole failed: %v", err)
	}

	if len(users) != 2 {
		t.Errorf("Expected 2 users with role, got %d", len(users))
	}

	// Verify correct users
	foundUser1, foundUser2 := false, false
	for _, u := range users {
		if u.Username == "user1" {
			foundUser1 = true
		}
		if u.Username == "user2" {
			foundUser2 = true
		}
	}
	if !foundUser1 || !foundUser2 {
		t.Error("Expected to find user1 and user2 with the role")
	}
}

func TestDatabaseScopedRoles(t *testing.T) {
	authMgr, cleanup := setupTestAuthManager(t)
	defer cleanup()

	// Create user and role
	authMgr.CreateUser("scoped_user", "password123")
	authMgr.CreateRole("db_role", "Database-scoped role", "admin")

	// Grant role scoped to a specific database
	err := authMgr.GrantRoleToUser("scoped_user", "db_role", "sales_db", "admin")
	if err != nil {
		t.Fatalf("GrantRoleToUser failed: %v", err)
	}

	// User should have role for sales_db
	if !authMgr.HasRole("scoped_user", "db_role", "sales_db") {
		t.Error("User should have role for sales_db")
	}

	// User should not have role globally
	if authMgr.HasRole("scoped_user", "db_role", "") {
		t.Error("User should not have role globally")
	}

	// User should not have role for other databases
	if authMgr.HasRole("scoped_user", "db_role", "other_db") {
		t.Error("User should not have role for other_db")
	}
}

func TestAnonymousUserDenied(t *testing.T) {
	authMgr, cleanup := setupTestAuthManager(t)
	defer cleanup()

	// Anonymous users (empty username) should be denied access
	check := authMgr.CheckPrivilege("", PrivilegeSelect, "default", "users")
	if check.Allowed {
		t.Error("Anonymous user should be denied access")
	}
	if check.GrantedBy != "denied" {
		t.Errorf("Expected GrantedBy 'denied', got '%s'", check.GrantedBy)
	}
}

func TestAdminUserHasAdminRole(t *testing.T) {
	authMgr, cleanup := setupTestAuthManager(t)
	defer cleanup()

	// Initialize built-in roles first (required for role assignment)
	err := authMgr.InitializeBuiltInRoles()
	if err != nil {
		t.Fatalf("InitializeBuiltInRoles failed: %v", err)
	}

	// Create admin user
	err = authMgr.CreateAdminUser("myadmin", "password123")
	if err != nil {
		t.Fatalf("CreateAdminUser failed: %v", err)
	}

	// Admin user should have the admin role
	if !authMgr.HasRole("myadmin", RoleAdmin, "") {
		t.Error("Admin user should have the admin role")
	}

	// Admin user should have all privileges
	check := authMgr.CheckPrivilege("myadmin", PrivilegeAll, "any_db", "any_table")
	if !check.Allowed {
		t.Error("Admin user should have all privileges")
	}
}

func TestRBACPrimaryAuthorization(t *testing.T) {
	authMgr, cleanup := setupTestAuthManager(t)
	defer cleanup()

	// Initialize built-in roles
	err := authMgr.InitializeBuiltInRoles()
	if err != nil {
		t.Fatalf("InitializeBuiltInRoles failed: %v", err)
	}

	// Create a regular user
	err = authMgr.CreateUser("testuser", "password123")
	if err != nil {
		t.Fatalf("CreateUser failed: %v", err)
	}

	// User without roles should be denied
	check := authMgr.CheckPrivilege("testuser", PrivilegeSelect, "default", "users")
	if check.Allowed {
		t.Error("User without roles should be denied access")
	}

	// Grant reader role to user
	err = authMgr.GrantRoleToUser("testuser", RoleReader, "", "admin")
	if err != nil {
		t.Fatalf("GrantRoleToUser failed: %v", err)
	}

	// User with reader role should have SELECT privilege
	check = authMgr.CheckPrivilege("testuser", PrivilegeSelect, "default", "users")
	if !check.Allowed {
		t.Error("User with reader role should have SELECT privilege")
	}
	if check.GrantedBy != "role" {
		t.Errorf("Expected GrantedBy 'role', got '%s'", check.GrantedBy)
	}

	// User with reader role should NOT have INSERT privilege
	check = authMgr.CheckPrivilege("testuser", PrivilegeInsert, "default", "users")
	if check.Allowed {
		t.Error("User with reader role should NOT have INSERT privilege")
	}
}

func TestHasDatabaseAccessWithRBAC(t *testing.T) {
	authMgr, cleanup := setupTestAuthManager(t)
	defer cleanup()

	// Initialize built-in roles
	authMgr.InitializeBuiltInRoles()

	// Create a user
	authMgr.CreateUser("dbuser", "password123")

	// User without roles should not have database access
	if authMgr.HasDatabaseAccess("dbuser", "testdb") {
		t.Error("User without roles should not have database access")
	}

	// Grant reader role to user
	authMgr.GrantRoleToUser("dbuser", RoleReader, "", "admin")

	// User with reader role should have database access
	if !authMgr.HasDatabaseAccess("dbuser", "testdb") {
		t.Error("User with reader role should have database access")
	}

	// Anonymous user should NOT have database access
	if authMgr.HasDatabaseAccess("", "testdb") {
		t.Error("Anonymous user should NOT have database access")
	}
}

func TestEnsureAdminHasRole(t *testing.T) {
	authMgr, cleanup := setupTestAuthManager(t)
	defer cleanup()

	// Initialize built-in roles first
	err := authMgr.InitializeBuiltInRoles()
	if err != nil {
		t.Fatalf("InitializeBuiltInRoles failed: %v", err)
	}

	// Create admin user WITHOUT the role (simulating old behavior)
	// We'll create a regular user first, then manually set IsAdmin
	err = authMgr.CreateUser("admin", "password123")
	if err != nil {
		t.Fatalf("CreateUser failed: %v", err)
	}

	// Admin should NOT have the role yet (created as regular user)
	if authMgr.HasRole("admin", RoleAdmin, "") {
		t.Error("Admin should not have admin role yet")
	}

	// Call EnsureAdminHasRole
	err = authMgr.EnsureAdminHasRole()
	if err != nil {
		t.Fatalf("EnsureAdminHasRole failed: %v", err)
	}

	// Now admin should have the role
	if !authMgr.HasRole("admin", RoleAdmin, "") {
		t.Error("Admin should have admin role after EnsureAdminHasRole")
	}

	// Calling again should be idempotent
	err = authMgr.EnsureAdminHasRole()
	if err != nil {
		t.Fatalf("EnsureAdminHasRole (second call) failed: %v", err)
	}
}
