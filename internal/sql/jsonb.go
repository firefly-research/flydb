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
Package sql contains JSONB operations for FlyDB.

JSONB Operators:
================

  - ->  : Get JSON object field by key (returns JSON)
  - ->> : Get JSON object field by key (returns text)
  - @>  : Contains (left contains right)
  - <@  : Contained by (left is contained by right)
  - ?   : Key exists
  - ?&  : All keys exist
  - ?|  : Any key exists

JSONB Functions:
================

  - json_extract(json, path)     : Extract value at JSON path
  - json_extract_text(json, path): Extract value as text
  - json_array_length(json)      : Get array length
  - json_keys(json)              : Get object keys as array
  - json_typeof(json)            : Get JSON value type
  - json_valid(json)             : Check if valid JSON
  - json_set(json, path, value)  : Set value at path
  - json_remove(json, path)      : Remove value at path
  - json_insert(json, path, val) : Insert value at path (if not exists)
  - json_replace(json, path, val): Replace value at path (if exists)
  - json_merge(json1, json2)     : Merge two JSON objects
  - json_array_append(json, val) : Append value to array
  - json_object(k1, v1, ...)     : Create JSON object from key-value pairs
  - json_array(v1, v2, ...)      : Create JSON array from values
*/
package sql

import (
	"encoding/json"
	"strconv"
	"strings"

	ferrors "flydb/internal/errors"
)

// JSONExtract extracts a value from a JSON document at the given path.
// Path syntax: $.key.subkey[0].field or key.subkey[0].field
// Returns the extracted value as JSON string, or empty string if not found.
func JSONExtract(jsonStr string, path string) (string, error) {
	if jsonStr == "" || path == "" {
		return "", nil
	}

	var data interface{}
	if err := json.Unmarshal([]byte(jsonStr), &data); err != nil {
		return "", ferrors.InvalidJSON(err.Error())
	}

	// Parse and navigate the path
	result, err := navigatePath(data, path)
	if err != nil {
		return "", err
	}

	if result == nil {
		return "null", nil
	}

	// Return as JSON
	resultBytes, err := json.Marshal(result)
	if err != nil {
		return "", err
	}
	return string(resultBytes), nil
}

// JSONExtractText extracts a value from a JSON document and returns it as text.
// For strings, returns the unquoted string. For other types, returns JSON representation.
func JSONExtractText(jsonStr string, path string) (string, error) {
	result, err := JSONExtract(jsonStr, path)
	if err != nil {
		return "", err
	}

	if result == "" || result == "null" {
		return "", nil
	}

	// If it's a string, unquote it
	if len(result) >= 2 && result[0] == '"' && result[len(result)-1] == '"' {
		var s string
		if err := json.Unmarshal([]byte(result), &s); err == nil {
			return s, nil
		}
	}

	return result, nil
}

// JSONContains checks if the left JSON contains the right JSON.
// For objects: all key-value pairs in right must exist in left.
// For arrays: all elements in right must exist in left.
func JSONContains(left, right string) (bool, error) {
	if left == "" || right == "" {
		return false, nil
	}

	var leftData, rightData interface{}
	if err := json.Unmarshal([]byte(left), &leftData); err != nil {
		return false, ferrors.InvalidJSON("invalid left JSON: " + err.Error())
	}
	if err := json.Unmarshal([]byte(right), &rightData); err != nil {
		return false, ferrors.InvalidJSON("invalid right JSON: " + err.Error())
	}

	return containsJSON(leftData, rightData), nil
}

// JSONContainedBy checks if the left JSON is contained by the right JSON.
func JSONContainedBy(left, right string) (bool, error) {
	return JSONContains(right, left)
}

// JSONKeyExists checks if a key exists in a JSON object.
func JSONKeyExists(jsonStr, key string) (bool, error) {
	if jsonStr == "" || key == "" {
		return false, nil
	}

	var data map[string]interface{}
	if err := json.Unmarshal([]byte(jsonStr), &data); err != nil {
		// Not an object
		return false, nil
	}

	_, exists := data[key]
	return exists, nil
}

// JSONAllKeysExist checks if all specified keys exist in a JSON object.
func JSONAllKeysExist(jsonStr string, keys []string) (bool, error) {
	if jsonStr == "" || len(keys) == 0 {
		return false, nil
	}

	var data map[string]interface{}
	if err := json.Unmarshal([]byte(jsonStr), &data); err != nil {
		return false, nil
	}

	for _, key := range keys {
		if _, exists := data[key]; !exists {
			return false, nil
		}
	}
	return true, nil
}

// JSONAnyKeyExists checks if any of the specified keys exist in a JSON object.
func JSONAnyKeyExists(jsonStr string, keys []string) (bool, error) {
	if jsonStr == "" || len(keys) == 0 {
		return false, nil
	}

	var data map[string]interface{}
	if err := json.Unmarshal([]byte(jsonStr), &data); err != nil {
		return false, nil
	}

	for _, key := range keys {
		if _, exists := data[key]; exists {
			return true, nil
		}
	}
	return false, nil
}

// JSONArrayLength returns the length of a JSON array.
func JSONArrayLength(jsonStr string) (int, error) {
	if jsonStr == "" {
		return 0, nil
	}

	var arr []interface{}
	if err := json.Unmarshal([]byte(jsonStr), &arr); err != nil {
		return 0, ferrors.InvalidJSON("not a JSON array")
	}

	return len(arr), nil
}

// JSONKeys returns the keys of a JSON object as a JSON array.
func JSONKeys(jsonStr string) (string, error) {
	if jsonStr == "" {
		return "[]", nil
	}

	var data map[string]interface{}
	if err := json.Unmarshal([]byte(jsonStr), &data); err != nil {
		return "", ferrors.InvalidJSON("not a JSON object")
	}

	keys := make([]string, 0, len(data))
	for k := range data {
		keys = append(keys, k)
	}

	result, err := json.Marshal(keys)
	if err != nil {
		return "", err
	}
	return string(result), nil
}

// JSONTypeOf returns the type of a JSON value.
func JSONTypeOf(jsonStr string) string {
	if jsonStr == "" {
		return "null"
	}

	var data interface{}
	if err := json.Unmarshal([]byte(jsonStr), &data); err != nil {
		return "invalid"
	}

	switch data.(type) {
	case nil:
		return "null"
	case bool:
		return "boolean"
	case float64:
		return "number"
	case string:
		return "string"
	case []interface{}:
		return "array"
	case map[string]interface{}:
		return "object"
	default:
		return "unknown"
	}
}

// JSONValid checks if a string is valid JSON.
func JSONValid(jsonStr string) bool {
	return json.Valid([]byte(jsonStr))
}

// JSONSet sets a value at the specified path in a JSON document.
func JSONSet(jsonStr, path string, value interface{}) (string, error) {
	if jsonStr == "" {
		jsonStr = "{}"
	}

	var data interface{}
	if err := json.Unmarshal([]byte(jsonStr), &data); err != nil {
		return "", ferrors.InvalidJSON(err.Error())
	}

	result, err := setPath(data, path, value)
	if err != nil {
		return "", err
	}

	resultBytes, err := json.Marshal(result)
	if err != nil {
		return "", err
	}
	return string(resultBytes), nil
}

// JSONRemove removes a value at the specified path in a JSON document.
func JSONRemove(jsonStr, path string) (string, error) {
	if jsonStr == "" {
		return "{}", nil
	}

	var data interface{}
	if err := json.Unmarshal([]byte(jsonStr), &data); err != nil {
		return "", ferrors.InvalidJSON(err.Error())
	}

	result, err := removePath(data, path)
	if err != nil {
		return "", err
	}

	resultBytes, err := json.Marshal(result)
	if err != nil {
		return "", err
	}
	return string(resultBytes), nil
}

// JSONMerge merges two JSON objects. Values from the second object override the first.
func JSONMerge(json1, json2 string) (string, error) {
	if json1 == "" {
		return json2, nil
	}
	if json2 == "" {
		return json1, nil
	}

	var data1, data2 map[string]interface{}
	if err := json.Unmarshal([]byte(json1), &data1); err != nil {
		return "", ferrors.InvalidJSON("first argument is not a JSON object")
	}
	if err := json.Unmarshal([]byte(json2), &data2); err != nil {
		return "", ferrors.InvalidJSON("second argument is not a JSON object")
	}

	// Merge data2 into data1
	for k, v := range data2 {
		data1[k] = v
	}

	result, err := json.Marshal(data1)
	if err != nil {
		return "", err
	}
	return string(result), nil
}

// JSONArrayAppend appends a value to a JSON array.
func JSONArrayAppend(jsonStr string, value interface{}) (string, error) {
	if jsonStr == "" {
		jsonStr = "[]"
	}

	var arr []interface{}
	if err := json.Unmarshal([]byte(jsonStr), &arr); err != nil {
		return "", ferrors.InvalidJSON("not a JSON array")
	}

	arr = append(arr, value)

	result, err := json.Marshal(arr)
	if err != nil {
		return "", err
	}
	return string(result), nil
}

// JSONObject creates a JSON object from key-value pairs.
func JSONObject(pairs ...string) (string, error) {
	if len(pairs)%2 != 0 {
		return "", ferrors.NewExecutionError("json_object requires an even number of arguments")
	}

	data := make(map[string]interface{})
	for i := 0; i < len(pairs); i += 2 {
		key := pairs[i]
		value := pairs[i+1]

		// Try to parse value as JSON
		var v interface{}
		if err := json.Unmarshal([]byte(value), &v); err != nil {
			// Use as string
			v = value
		}
		data[key] = v
	}

	result, err := json.Marshal(data)
	if err != nil {
		return "", err
	}
	return string(result), nil
}

// JSONArray creates a JSON array from values.
func JSONArray(values ...string) (string, error) {
	arr := make([]interface{}, len(values))
	for i, value := range values {
		// Try to parse value as JSON
		var v interface{}
		if err := json.Unmarshal([]byte(value), &v); err != nil {
			// Use as string
			v = value
		}
		arr[i] = v
	}

	result, err := json.Marshal(arr)
	if err != nil {
		return "", err
	}
	return string(result), nil
}

// navigatePath navigates a JSON structure using a path expression.
// Path syntax: $.key.subkey[0].field or key.subkey[0].field
func navigatePath(data interface{}, path string) (interface{}, error) {
	// Remove leading $. if present
	path = strings.TrimPrefix(path, "$.")
	path = strings.TrimPrefix(path, "$")

	if path == "" {
		return data, nil
	}

	parts := parsePathParts(path)
	current := data

	for _, part := range parts {
		if current == nil {
			return nil, nil
		}

		// Check if this is an array index
		if strings.HasPrefix(part, "[") && strings.HasSuffix(part, "]") {
			indexStr := part[1 : len(part)-1]
			index, err := strconv.Atoi(indexStr)
			if err != nil {
				return nil, ferrors.JSONPathError("invalid array index: " + indexStr)
			}

			arr, ok := current.([]interface{})
			if !ok {
				return nil, nil
			}

			if index < 0 || index >= len(arr) {
				return nil, nil
			}
			current = arr[index]
		} else {
			// Object key
			obj, ok := current.(map[string]interface{})
			if !ok {
				return nil, nil
			}

			val, exists := obj[part]
			if !exists {
				return nil, nil
			}
			current = val
		}
	}

	return current, nil
}

// parsePathParts splits a JSON path into its component parts.
func parsePathParts(path string) []string {
	var parts []string
	var current strings.Builder
	inBracket := false

	for _, ch := range path {
		switch ch {
		case '.':
			if !inBracket && current.Len() > 0 {
				parts = append(parts, current.String())
				current.Reset()
			} else if inBracket {
				current.WriteRune(ch)
			}
		case '[':
			if current.Len() > 0 {
				parts = append(parts, current.String())
				current.Reset()
			}
			current.WriteRune(ch)
			inBracket = true
		case ']':
			current.WriteRune(ch)
			parts = append(parts, current.String())
			current.Reset()
			inBracket = false
		default:
			current.WriteRune(ch)
		}
	}

	if current.Len() > 0 {
		parts = append(parts, current.String())
	}

	return parts
}

// containsJSON checks if left contains right (recursive).
func containsJSON(left, right interface{}) bool {
	switch r := right.(type) {
	case map[string]interface{}:
		l, ok := left.(map[string]interface{})
		if !ok {
			return false
		}
		for k, rv := range r {
			lv, exists := l[k]
			if !exists {
				return false
			}
			if !containsJSON(lv, rv) {
				return false
			}
		}
		return true

	case []interface{}:
		l, ok := left.([]interface{})
		if !ok {
			return false
		}
		// Each element in right must exist in left
		for _, rv := range r {
			found := false
			for _, lv := range l {
				if containsJSON(lv, rv) {
					found = true
					break
				}
			}
			if !found {
				return false
			}
		}
		return true

	default:
		// Primitive comparison
		return left == right
	}
}

// setPath sets a value at the specified path in a JSON structure.
func setPath(data interface{}, path string, value interface{}) (interface{}, error) {
	path = strings.TrimPrefix(path, "$.")
	path = strings.TrimPrefix(path, "$")

	if path == "" {
		return value, nil
	}

	parts := parsePathParts(path)
	return setPathRecursive(data, parts, value)
}

func setPathRecursive(data interface{}, parts []string, value interface{}) (interface{}, error) {
	if len(parts) == 0 {
		return value, nil
	}

	part := parts[0]
	remaining := parts[1:]

	// Check if this is an array index
	if strings.HasPrefix(part, "[") && strings.HasSuffix(part, "]") {
		indexStr := part[1 : len(part)-1]
		index, err := strconv.Atoi(indexStr)
		if err != nil {
			return nil, ferrors.JSONPathError("invalid array index: " + indexStr)
		}

		arr, ok := data.([]interface{})
		if !ok {
			arr = make([]interface{}, index+1)
		}

		// Extend array if needed
		for len(arr) <= index {
			arr = append(arr, nil)
		}

		if len(remaining) == 0 {
			arr[index] = value
		} else {
			newVal, err := setPathRecursive(arr[index], remaining, value)
			if err != nil {
				return nil, err
			}
			arr[index] = newVal
		}
		return arr, nil
	}

	// Object key
	obj, ok := data.(map[string]interface{})
	if !ok {
		obj = make(map[string]interface{})
	}

	if len(remaining) == 0 {
		obj[part] = value
	} else {
		existing := obj[part]
		newVal, err := setPathRecursive(existing, remaining, value)
		if err != nil {
			return nil, err
		}
		obj[part] = newVal
	}
	return obj, nil
}

// removePath removes a value at the specified path in a JSON structure.
func removePath(data interface{}, path string) (interface{}, error) {
	path = strings.TrimPrefix(path, "$.")
	path = strings.TrimPrefix(path, "$")

	if path == "" {
		return nil, nil
	}

	parts := parsePathParts(path)
	return removePathRecursive(data, parts)
}

func removePathRecursive(data interface{}, parts []string) (interface{}, error) {
	if len(parts) == 0 {
		return data, nil
	}

	part := parts[0]
	remaining := parts[1:]

	// Check if this is an array index
	if strings.HasPrefix(part, "[") && strings.HasSuffix(part, "]") {
		indexStr := part[1 : len(part)-1]
		index, err := strconv.Atoi(indexStr)
		if err != nil {
			return nil, ferrors.JSONPathError("invalid array index: " + indexStr)
		}

		arr, ok := data.([]interface{})
		if !ok {
			return data, nil
		}

		if index < 0 || index >= len(arr) {
			return data, nil
		}

		if len(remaining) == 0 {
			// Remove element at index
			return append(arr[:index], arr[index+1:]...), nil
		}

		newVal, err := removePathRecursive(arr[index], remaining)
		if err != nil {
			return nil, err
		}
		arr[index] = newVal
		return arr, nil
	}

	// Object key
	obj, ok := data.(map[string]interface{})
	if !ok {
		return data, nil
	}

	if len(remaining) == 0 {
		delete(obj, part)
		return obj, nil
	}

	existing, exists := obj[part]
	if !exists {
		return obj, nil
	}

	newVal, err := removePathRecursive(existing, remaining)
	if err != nil {
		return nil, err
	}
	obj[part] = newVal
	return obj, nil
}

// JSONGetField gets a field from a JSON object by key (-> operator).
// Returns the value as JSON.
func JSONGetField(jsonStr, key string) (string, error) {
	if jsonStr == "" || key == "" {
		return "", nil
	}

	var data map[string]interface{}
	if err := json.Unmarshal([]byte(jsonStr), &data); err != nil {
		// Try as array with numeric index
		var arr []interface{}
		if err := json.Unmarshal([]byte(jsonStr), &arr); err != nil {
			return "", nil
		}
		index, err := strconv.Atoi(key)
		if err != nil || index < 0 || index >= len(arr) {
			return "", nil
		}
		result, _ := json.Marshal(arr[index])
		return string(result), nil
	}

	val, exists := data[key]
	if !exists {
		return "", nil
	}

	result, err := json.Marshal(val)
	if err != nil {
		return "", err
	}
	return string(result), nil
}

// JSONGetFieldText gets a field from a JSON object by key (->> operator).
// Returns the value as text (unquoted for strings).
func JSONGetFieldText(jsonStr, key string) (string, error) {
	result, err := JSONGetField(jsonStr, key)
	if err != nil || result == "" {
		return "", err
	}

	// If it's a string, unquote it
	if len(result) >= 2 && result[0] == '"' && result[len(result)-1] == '"' {
		var s string
		if err := json.Unmarshal([]byte(result), &s); err == nil {
			return s, nil
		}
	}

	return result, nil
}
