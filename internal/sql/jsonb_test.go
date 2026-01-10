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

package sql

import (
	"testing"
)

// TestJSONExtract tests the JSONExtract function
func TestJSONExtract(t *testing.T) {
	tests := []struct {
		name     string
		json     string
		path     string
		expected string
		wantErr  bool
	}{
		{"simple key", `{"name": "John"}`, "name", `"John"`, false},
		{"nested key", `{"user": {"name": "John"}}`, "user.name", `"John"`, false},
		{"array index", `{"items": [1, 2, 3]}`, "items[0]", "1", false},
		{"nested array", `{"users": [{"name": "John"}]}`, "users[0].name", `"John"`, false},
		{"with dollar prefix", `{"name": "John"}`, "$.name", `"John"`, false},
		{"null value", `{"name": null}`, "name", "null", false},
		{"missing key", `{"name": "John"}`, "age", "null", false},
		{"empty json", "", "name", "", false},
		{"empty path", `{"name": "John"}`, "", "", false},
		{"invalid json", "not json", "name", "", true},
		{"number value", `{"age": 30}`, "age", "30", false},
		{"boolean value", `{"active": true}`, "active", "true", false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := JSONExtract(tt.json, tt.path)
			if (err != nil) != tt.wantErr {
				t.Errorf("JSONExtract() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if result != tt.expected {
				t.Errorf("JSONExtract() = %v, want %v", result, tt.expected)
			}
		})
	}
}

// TestJSONExtractText tests the JSONExtractText function
func TestJSONExtractText(t *testing.T) {
	tests := []struct {
		name     string
		json     string
		path     string
		expected string
	}{
		{"string value", `{"name": "John"}`, "name", "John"},
		{"number value", `{"age": 30}`, "age", "30"},
		{"boolean value", `{"active": true}`, "active", "true"},
		{"nested string", `{"user": {"name": "John"}}`, "user.name", "John"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := JSONExtractText(tt.json, tt.path)
			if err != nil {
				t.Errorf("JSONExtractText() error = %v", err)
				return
			}
			if result != tt.expected {
				t.Errorf("JSONExtractText() = %v, want %v", result, tt.expected)
			}
		})
	}
}

// TestJSONContains tests the JSONContains function
func TestJSONContains(t *testing.T) {
	tests := []struct {
		name     string
		left     string
		right    string
		expected bool
	}{
		{"object contains key-value", `{"name": "John", "age": 30}`, `{"name": "John"}`, true},
		{"object missing key", `{"name": "John"}`, `{"age": 30}`, false},
		{"array contains element", `[1, 2, 3]`, `[2]`, true},
		{"array missing element", `[1, 2, 3]`, `[4]`, false},
		{"nested object", `{"user": {"name": "John", "age": 30}}`, `{"user": {"name": "John"}}`, true},
		{"empty right", `{"name": "John"}`, `{}`, true},
		{"equal objects", `{"a": 1}`, `{"a": 1}`, true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := JSONContains(tt.left, tt.right)
			if err != nil {
				t.Errorf("JSONContains() error = %v", err)
				return
			}
			if result != tt.expected {
				t.Errorf("JSONContains() = %v, want %v", result, tt.expected)
			}
		})
	}
}

// TestJSONKeyExists tests the JSONKeyExists function
func TestJSONKeyExists(t *testing.T) {
	tests := []struct {
		name     string
		json     string
		key      string
		expected bool
	}{
		{"key exists", `{"name": "John", "age": 30}`, "name", true},
		{"key missing", `{"name": "John"}`, "age", false},
		{"null value key", `{"name": null}`, "name", true},
		{"empty object", `{}`, "name", false},
		{"not an object", `[1, 2, 3]`, "0", false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := JSONKeyExists(tt.json, tt.key)
			if err != nil {
				t.Errorf("JSONKeyExists() error = %v", err)
				return
			}
			if result != tt.expected {
				t.Errorf("JSONKeyExists() = %v, want %v", result, tt.expected)
			}
		})
	}
}

// TestJSONAllKeysExist tests the JSONAllKeysExist function
func TestJSONAllKeysExist(t *testing.T) {
	tests := []struct {
		name     string
		json     string
		keys     []string
		expected bool
	}{
		{"all keys exist", `{"name": "John", "age": 30, "city": "NYC"}`, []string{"name", "age"}, true},
		{"one key missing", `{"name": "John", "age": 30}`, []string{"name", "city"}, false},
		{"all keys missing", `{"name": "John"}`, []string{"age", "city"}, false},
		{"empty keys", `{"name": "John"}`, []string{}, false},
		{"single key exists", `{"name": "John"}`, []string{"name"}, true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := JSONAllKeysExist(tt.json, tt.keys)
			if err != nil {
				t.Errorf("JSONAllKeysExist() error = %v", err)
				return
			}
			if result != tt.expected {
				t.Errorf("JSONAllKeysExist() = %v, want %v", result, tt.expected)
			}
		})
	}
}

// TestJSONAnyKeyExists tests the JSONAnyKeyExists function
func TestJSONAnyKeyExists(t *testing.T) {
	tests := []struct {
		name     string
		json     string
		keys     []string
		expected bool
	}{
		{"one key exists", `{"name": "John", "age": 30}`, []string{"name", "city"}, true},
		{"no keys exist", `{"name": "John"}`, []string{"age", "city"}, false},
		{"all keys exist", `{"name": "John", "age": 30}`, []string{"name", "age"}, true},
		{"empty keys", `{"name": "John"}`, []string{}, false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := JSONAnyKeyExists(tt.json, tt.keys)
			if err != nil {
				t.Errorf("JSONAnyKeyExists() error = %v", err)
				return
			}
			if result != tt.expected {
				t.Errorf("JSONAnyKeyExists() = %v, want %v", result, tt.expected)
			}
		})
	}
}

// TestJSONArrayLength tests the JSONArrayLength function
func TestJSONArrayLength(t *testing.T) {
	tests := []struct {
		name     string
		json     string
		expected int
		wantErr  bool
	}{
		{"simple array", `[1, 2, 3]`, 3, false},
		{"empty array", `[]`, 0, false},
		{"nested array", `[[1, 2], [3, 4]]`, 2, false},
		{"not an array", `{"name": "John"}`, 0, true},
		{"empty string", "", 0, false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := JSONArrayLength(tt.json)
			if (err != nil) != tt.wantErr {
				t.Errorf("JSONArrayLength() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if result != tt.expected {
				t.Errorf("JSONArrayLength() = %v, want %v", result, tt.expected)
			}
		})
	}
}

// TestJSONKeys tests the JSONKeys function
func TestJSONKeys(t *testing.T) {
	tests := []struct {
		name    string
		json    string
		wantErr bool
	}{
		{"simple object", `{"name": "John", "age": 30}`, false},
		{"empty object", `{}`, false},
		{"not an object", `[1, 2, 3]`, true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := JSONKeys(tt.json)
			if (err != nil) != tt.wantErr {
				t.Errorf("JSONKeys() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !tt.wantErr && result == "" {
				t.Errorf("JSONKeys() returned empty result for valid object")
			}
		})
	}
}

// TestJSONTypeOf tests the JSONTypeOf function
func TestJSONTypeOf(t *testing.T) {
	tests := []struct {
		name     string
		json     string
		expected string
	}{
		{"object", `{"name": "John"}`, "object"},
		{"array", `[1, 2, 3]`, "array"},
		{"string", `"hello"`, "string"},
		{"number", `42`, "number"},
		{"boolean true", `true`, "boolean"},
		{"boolean false", `false`, "boolean"},
		{"null", `null`, "null"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := JSONTypeOf(tt.json)
			if result != tt.expected {
				t.Errorf("JSONTypeOf() = %v, want %v", result, tt.expected)
			}
		})
	}
}

// TestJSONValid tests the JSONValid function
func TestJSONValid(t *testing.T) {
	tests := []struct {
		name     string
		json     string
		expected bool
	}{
		{"valid object", `{"name": "John"}`, true},
		{"valid array", `[1, 2, 3]`, true},
		{"valid string", `"hello"`, true},
		{"valid number", `42`, true},
		{"valid null", `null`, true},
		{"invalid json", `{name: John}`, false},
		{"empty string", ``, false},
		{"incomplete object", `{"name":`, false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := JSONValid(tt.json)
			if result != tt.expected {
				t.Errorf("JSONValid() = %v, want %v", result, tt.expected)
			}
		})
	}

}

// TestJSONSet tests the JSONSet function
func TestJSONSet(t *testing.T) {
	tests := []struct {
		name     string
		json     string
		path     string
		value    interface{}
		expected string
		wantErr  bool
	}{
		{"set existing key", `{"name": "John"}`, "name", "Jane", `{"name":"Jane"}`, false},
		{"set new key", `{"name": "John"}`, "age", 30, `{"age":30,"name":"John"}`, false},
		{"set nested key", `{"user": {"name": "John"}}`, "user.age", 30, `{"user":{"age":30,"name":"John"}}`, false},
		{"set array element", `{"items": [1, 2, 3]}`, "items[1]", 5, `{"items":[1,5,3]}`, false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := JSONSet(tt.json, tt.path, tt.value)
			if (err != nil) != tt.wantErr {
				t.Errorf("JSONSet() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if result != tt.expected {
				t.Errorf("JSONSet() = %v, want %v", result, tt.expected)
			}
		})
	}
}

// TestJSONRemove tests the JSONRemove function
func TestJSONRemove(t *testing.T) {
	tests := []struct {
		name     string
		json     string
		path     string
		expected string
		wantErr  bool
	}{
		{"remove key", `{"name": "John", "age": 30}`, "age", `{"name":"John"}`, false},
		{"remove nested key", `{"user": {"name": "John", "age": 30}}`, "user.age", `{"user":{"name":"John"}}`, false},
		{"remove missing key", `{"name": "John"}`, "age", `{"name":"John"}`, false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := JSONRemove(tt.json, tt.path)
			if (err != nil) != tt.wantErr {
				t.Errorf("JSONRemove() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if result != tt.expected {
				t.Errorf("JSONRemove() = %v, want %v", result, tt.expected)
			}
		})
	}
}

// TestJSONMerge tests the JSONMerge function
func TestJSONMerge(t *testing.T) {
	tests := []struct {
		name     string
		json1    string
		json2    string
		expected string
		wantErr  bool
	}{
		{"merge objects", `{"name": "John"}`, `{"age": 30}`, `{"age":30,"name":"John"}`, false},
		{"merge with override", `{"name": "John"}`, `{"name": "Jane"}`, `{"name":"Jane"}`, false},
		{"merge nested", `{"user": {"name": "John"}}`, `{"user": {"age": 30}}`, `{"user":{"age":30}}`, false},
		{"merge empty", `{"name": "John"}`, `{}`, `{"name":"John"}`, false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := JSONMerge(tt.json1, tt.json2)
			if (err != nil) != tt.wantErr {
				t.Errorf("JSONMerge() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if result != tt.expected {
				t.Errorf("JSONMerge() = %v, want %v", result, tt.expected)
			}
		})
	}
}

// TestJSONArrayAppend tests the JSONArrayAppend function
func TestJSONArrayAppend(t *testing.T) {
	tests := []struct {
		name     string
		json     string
		value    interface{}
		expected string
		wantErr  bool
	}{
		{"append to array", `[1, 2, 3]`, 4, `[1,2,3,4]`, false},
		{"append string", `["a", "b"]`, "c", `["a","b","c"]`, false},
		{"append object", `[{"a": 1}]`, map[string]interface{}{"b": 2}, `[{"a":1},{"b":2}]`, false},
		{"append to empty", `[]`, 1, `[1]`, false},
		{"not an array", `{"a": 1}`, 2, "", true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := JSONArrayAppend(tt.json, tt.value)
			if (err != nil) != tt.wantErr {
				t.Errorf("JSONArrayAppend() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if result != tt.expected {
				t.Errorf("JSONArrayAppend() = %v, want %v", result, tt.expected)
			}
		})
	}
}

// TestJSONObject tests the JSONObject function
func TestJSONObject(t *testing.T) {
	tests := []struct {
		name     string
		args     []string
		expected string
		wantErr  bool
	}{
		{"simple object", []string{"name", "John", "age", "30"}, `{"age":30,"name":"John"}`, false},
		{"empty object", []string{}, `{}`, false},
		{"single pair", []string{"key", "value"}, `{"key":"value"}`, false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := JSONObject(tt.args...)
			if (err != nil) != tt.wantErr {
				t.Errorf("JSONObject() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if result != tt.expected {
				t.Errorf("JSONObject() = %v, want %v", result, tt.expected)
			}
		})
	}
}

// TestJSONGetField tests the JSONGetField function (-> operator)
func TestJSONGetField(t *testing.T) {
	tests := []struct {
		name     string
		json     string
		key      string
		expected string
	}{
		{"get string field", `{"name": "John"}`, "name", `"John"`},
		{"get number field", `{"age": 30}`, "age", "30"},
		{"get object field", `{"user": {"name": "John"}}`, "user", `{"name":"John"}`},
		{"get array field", `{"items": [1, 2, 3]}`, "items", "[1,2,3]"},
		{"get missing field", `{"name": "John"}`, "age", ""},
		{"get array index", `[1, 2, 3]`, "1", "2"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := JSONGetField(tt.json, tt.key)
			if err != nil {
				t.Errorf("JSONGetField() error = %v", err)
				return
			}
			if result != tt.expected {
				t.Errorf("JSONGetField() = %v, want %v", result, tt.expected)
			}
		})
	}
}

// TestJSONGetFieldText tests the JSONGetFieldText function (->> operator)
func TestJSONGetFieldText(t *testing.T) {
	tests := []struct {
		name     string
		json     string
		key      string
		expected string
	}{
		{"get string as text", `{"name": "John"}`, "name", "John"},
		{"get number as text", `{"age": 30}`, "age", "30"},
		{"get boolean as text", `{"active": true}`, "active", "true"},
		{"get null as text", `{"value": null}`, "value", "null"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := JSONGetFieldText(tt.json, tt.key)
			if err != nil {
				t.Errorf("JSONGetFieldText() error = %v", err)
				return
			}
			if result != tt.expected {
				t.Errorf("JSONGetFieldText() = %v, want %v", result, tt.expected)
			}
		})
	}
}
