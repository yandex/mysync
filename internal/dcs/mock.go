package dcs

import (
	"errors"
	"fmt"
	"reflect"
	"strings"
	"time"
)

var ErrUnreachable = errors.New("DCS is unreachable")

type MockDCS struct {
	path        map[string]string
	locks       map[string]string
	Unreachable bool
	HostToLock  string
}

func NewMockDCS() *MockDCS {
	return &MockDCS{
		path:        make(map[string]string),
		locks:       make(map[string]string),
		Unreachable: false,
	}
}

func (mdcs *MockDCS) IsConnected() bool {
	return mdcs.isUnreachable()
}

func (mdcs *MockDCS) WaitConnected(timeout time.Duration) bool {
	return mdcs.isUnreachable()
}

func (mdcs *MockDCS) Initialize() {}

func (mdcs *MockDCS) SetDisconnectCallback(callback func() error) {}

func (mdcs *MockDCS) AcquireLock(path string) bool {
	if mdcs.isUnreachable() {
		return false
	}

	if owner, ok := mdcs.locks[path]; ok && owner != mdcs.HostToLock {
		return false
	}
	mdcs.locks[path] = mdcs.HostToLock
	return true
}

func (mdcs *MockDCS) ReleaseLock(path string) {
	if mdcs.isUnreachable() {
		return
	}

	if owner, ok := mdcs.locks[path]; ok && owner == mdcs.HostToLock {
		delete(mdcs.locks, path)
	}
}

func (mdcs *MockDCS) Create(path string, value any) error {
	if mdcs.isUnreachable() {
		return ErrUnreachable
	}

	if _, ok := mdcs.path[path]; ok {
		return ErrExists
	}
	parent := parentPath(path)
	parent = strings.TrimLeft(parent, "/")
	if parent != "" && parent != "/" {
		if _, ok := mdcs.path[parent]; !ok {
			return ErrNotFound
		}
	}
	mdcs.path[path] = fmt.Sprintf("%v", value)
	return nil
}

func (mdcs *MockDCS) CreateEphemeral(path string, value any) error {
	return mdcs.Create(path, value)
}

func (mdcs *MockDCS) Set(path string, value any) error {
	if mdcs.isUnreachable() {
		return ErrUnreachable
	}
	if _, ok := mdcs.path[path]; !ok {
		return ErrNotFound
	}
	mdcs.path[path] = fmt.Sprintf("%v", value)
	return nil
}

func (mdcs *MockDCS) SetEphemeral(path string, value any) error {
	return mdcs.Set(path, value)
}

func (mdcs *MockDCS) Get(path string, dest any) error {
	if mdcs.isUnreachable() {
		return ErrUnreachable
	}

	v, ok := mdcs.path[path]
	if !ok {
		return ErrNotFound
	}
	destVal := reflect.ValueOf(dest)
	if destVal.Kind() != reflect.Ptr || destVal.IsNil() {
		return errors.New("dest must be a non-nil pointer")
	}
	destElem := destVal.Elem()
	if !reflect.ValueOf(v).Type().AssignableTo(destElem.Type()) {
		return errors.New("type mismatch")
	}
	destElem.Set(reflect.ValueOf(v))
	return nil
}

func (mdcs *MockDCS) Delete(path string) error {
	if mdcs.isUnreachable() {
		return ErrUnreachable
	}

	if _, ok := mdcs.path[path]; !ok {
		return ErrNotFound
	}
	children, _ := mdcs.GetChildren(path)
	if len(children) > 0 {
		return errors.New("cannot delete node with children")
	}
	delete(mdcs.path, path)
	delete(mdcs.locks, path)
	return nil
}

func (mdcs *MockDCS) GetTree(path string) (any, error) {
	if mdcs.isUnreachable() {
		return nil, ErrUnreachable
	}

	// Recursive helper to build the subtree.
	var build func(p string) (any, error)
	build = func(p string) (any, error) {
		// Try to get children first (exists check also implies get-tree behavior).
		children, err := mdcs.GetChildren(p)
		if err != nil {
			return nil, err
		}

		if len(children) == 0 {
			// Leaf (or non-existent if value missing).
			var v any
			if err := mdcs.Get(p, &v); err != nil {
				if errors.Is(err, ErrNotFound) {
					return nil, ErrNotFound
				}
				return nil, err
			}
			return v, nil
		}

		// Internal node: build a map of childName -> childSubtree
		subtree := make(map[string]any, len(children))
		for _, c := range children {
			childPath := concatPath(p, c)
			v, err := build(childPath)
			if err != nil {
				return nil, err
			}
			subtree[c] = v
		}

		// Note: if the current node also has a value and you need it returned,
		// you could include it here under a reserved key (e.g. "_value").
		return subtree, nil
	}

	return build(path)
}

// concatPath builds a child path consistent with GetChildren prefixing logic.
func concatPath(parent, child string) string {
	prefix := parent
	if prefix != "/" && prefix != "" {
		if !strings.HasSuffix(prefix, "/") {
			prefix += "/"
		}
	} else {
		prefix = "/"
	}
	return prefix + child
}

func (mdcs *MockDCS) GetChildren(path string) ([]string, error) {
	if mdcs.isUnreachable() {
		return nil, ErrUnreachable
	}

	children := []string{}
	prefix := path
	if prefix != "/" && prefix != "" {
		if !strings.HasSuffix(prefix, "/") {
			prefix += "/"
		}
	} else {
		prefix = "/"
	}
	for k := range mdcs.path {
		if strings.HasPrefix(k, prefix) && k != prefix {
			sub := strings.TrimPrefix(k, prefix)
			if !strings.Contains(sub, "/") {
				children = append(children, sub)
			}
		}
	}
	return children, nil
}

func (mdcs *MockDCS) Close() {}

func (mdcs *MockDCS) isUnreachable() bool {
	return mdcs.Unreachable
}

func parentPath(path string) string {
	if path == "/" || path == "" {
		return ""
	}
	parts := strings.Split(strings.TrimRight(path, "/"), "/")
	if len(parts) == 1 {
		return "/"
	}
	return "/" + strings.Join(parts[:len(parts)-1], "/")
}
