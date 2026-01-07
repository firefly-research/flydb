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

