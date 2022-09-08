// Package filecache provides an LRU cache of opened files. If the same files
// are frequently opened and closed this is useful for reducing the number of
// syscalls for opening and closing the files.
package filecache

import (
	"container/list"
	"os"
	"sync"
)

type FileCache struct {
	cache     map[string]*list.Element
	capacity  int
	ll        *list.List
	lock      sync.Mutex
	onEvicted func(*os.File, int)
	openFlag  int
	openPerm  os.FileMode
	removed   map[*os.File]int

	// Stats
	hit  int
	miss int
}

type entry struct {
	file *os.File
	refs int
}

// New creates a new FileCache that can hold up to specified capacity of open
// files. If capacity is 0, then there is no limit. Files are opened read-only.
// If other open flags and permissions are needed, use NewOpenFile.
func New(capacity int) *FileCache {
	return NewOpenFile(capacity, os.O_RDONLY, 0)
}

// NewOpenFile created a new FileCache that opens files using the specified
// arguments to os.OpenFile.
func NewOpenFile(capacity int, openFlag int, openPerm os.FileMode) *FileCache {
	return &FileCache{
		capacity: capacity,
		openFlag: openFlag,
		openPerm: openPerm,
	}
}

// Open returns the already opened file, or opens the named file and returns
// that. The file is subsequently retrievable without opening it again, unless
// it has been removed from the FileCache.
//
// Every call to Open must be accompanied by a call to Close. Otherwise,
// reference counts will not be adjusted correctly and file handles will leak.
func (c *FileCache) Open(name string) (*os.File, error) {
	c.lock.Lock()
	defer c.lock.Unlock()

	if c.capacity == 0 {
		return os.OpenFile(name, c.openFlag, c.openPerm)
	}

	if c.cache == nil {
		c.cache = make(map[string]*list.Element)
		c.ll = list.New()
	}

	if elem, ok := c.cache[name]; ok {
		c.ll.MoveToFront(elem)
		ent := elem.Value.(*entry)
		ent.refs++
		c.hit++
		return ent.file, nil
	}
	c.miss++

	file, err := os.OpenFile(name, c.openFlag, c.openPerm)
	if err != nil {
		return nil, err
	}

	c.cache[name] = c.ll.PushFront(&entry{file, 1})
	if c.capacity != 0 && c.ll.Len() > c.capacity {
		c.removeOldest()
	}

	return file, nil
}

// Close decrements the reference count on the file. If the file has been
// removed from the cache and the reference count is zero, then the file is
// closed.
func (c *FileCache) Close(file *os.File) error {
	name := file.Name()

	c.lock.Lock()
	defer c.lock.Unlock()

	if elem, ok := c.cache[name]; ok {
		ent := elem.Value.(*entry)
		if ent.refs == 0 {
			return &os.PathError{Op: "close", Path: name, Err: os.ErrClosed}
		}
		ent.refs--
		return nil
	}
	// File is no longer in cache, see if it was removed.
	refs, ok := c.removed[file]
	if !ok {
		return file.Close()
	}

	if refs == 1 {
		delete(c.removed, file)
		if len(c.removed) == 0 {
			c.removed = nil
		}
		return file.Close()
	}

	// Removed from cache, but still in use.
	c.removed[file] = refs - 1
	return nil
}

// Len return the number of open files in the FileCache.
func (c *FileCache) Len() int {
	c.lock.Lock()
	defer c.lock.Unlock()

	if c.cache == nil {
		return 0
	}
	return c.ll.Len()
}

// Capacity return the capacity of the FileCache.
func (c *FileCache) Cap() int {
	c.lock.Lock()
	defer c.lock.Unlock()

	return c.capacity
}

// Clear closes and removes all files in the FileCache.
func (c *FileCache) Clear() {
	c.lock.Lock()
	defer c.lock.Unlock()

	for _, elem := range c.cache {
		c.removeElement(elem)
	}
	c.ll = nil
	c.cache = nil
}

func (c *FileCache) Remove(name string) {
	c.lock.Lock()
	defer c.lock.Unlock()

	if elem, ok := c.cache[name]; ok {
		c.removeElement(elem)
	}
}

func (c *FileCache) SetCacheSize(capacity int) {
	c.lock.Lock()
	defer c.lock.Unlock()

	if capacity < c.capacity {
		if capacity == 0 {
			for _, elem := range c.cache {
				c.removeElement(elem)
			}
			c.ll = nil
			c.cache = nil
		} else {
			for i := capacity; i < c.capacity; i++ {
				c.removeOldest()
			}
		}
	}
	c.capacity = capacity
}

func (c *FileCache) SetOnEvicted(f func(*os.File, int)) {
	c.lock.Lock()
	defer c.lock.Unlock()

	c.onEvicted = f
}

func (c *FileCache) Stats() (int, int, int, int) {
	c.lock.Lock()
	defer c.lock.Unlock()

	var items int
	if c.cache != nil {
		items = c.ll.Len()
	}
	// If exceeded max, reset.
	if c.hit < 0 || c.miss < 0 {
		c.hit = 0
		c.miss = 0
	}
	return c.hit, c.miss, items, c.capacity
}

func (c *FileCache) removeOldest() {
	elem := c.ll.Back()
	if elem != nil {
		c.removeElement(elem)
	}
}

func (c *FileCache) removeElement(elem *list.Element) {
	c.ll.Remove(elem)
	ent := elem.Value.(*entry)
	delete(c.cache, ent.file.Name())
	if c.onEvicted != nil {
		c.onEvicted(ent.file, ent.refs)
	}
	if ent.refs == 0 {
		ent.file.Close()
		return
	}
	// Removed from cache, but still in use.
	if c.removed == nil {
		c.removed = make(map[*os.File]int)
	}
	c.removed[ent.file] = ent.refs
}
