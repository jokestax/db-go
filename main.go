package main

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sync"

	"github.com/charmbracelet/log"
	"github.com/jcelliott/lumber"
)

type Address struct {
	City    string
	State   string
	Country string
	Pincode json.Number
}

type User struct {
	Name    string
	Age     json.Number
	Contact string
	Address Address
}

type (
	Logger interface {
		Fatal(string, ...interface{})
		Error(string, ...interface{})
		Warn(string, ...interface{})
		Info(string, ...interface{})
		Debug(string, ...interface{})
		Trace(string, ...interface{})
	}

	Driver struct {
		mutex   sync.Mutex
		mutexex map[string]*sync.Mutex
		dir     string
		log     Logger
	}
)

type Options struct {
	Logger Logger
}

func New(dir string, options *Options) (*Driver, error) {
	dir = filepath.Clean(dir)

	opts := Options{}

	if options != nil {
		opts = *options
	}

	if opts.Logger == nil {
		opts.Logger = lumber.NewConsoleLogger((lumber.INFO))
	}

	driver := Driver{
		dir:     dir,
		log:     opts.Logger,
		mutexex: make(map[string]*sync.Mutex),
	}

	if _, err := os.Stat(dir); err == nil {
		opts.Logger.Debug("using %s database already exists", dir)
		return &driver, nil
	}

	opts.Logger.Debug("Creating Database ....")
	return &driver, os.MkdirAll(dir, 0755)
}

func (d *Driver) Write(collection, resource string, v interface{}) error {
	if collection == "" {
		return fmt.Errorf("Missing collection - no place to save record!")
	}

	if resource == "" {
		return fmt.Errorf("Missing resource - unable to save record (no name)!")
	}

	mutex := d.getOrCreateMutex(collection)
	mutex.Lock()
	defer mutex.Unlock()

	dir := filepath.Join(d.dir, collection)
	fnlPath := filepath.Join(dir, resource+".json")
	tmpPath := fnlPath + ".tmp"

	if err := os.MkdirAll(dir, 0755); err != nil {
		return err
	}

	b, err := json.MarshalIndent(v, "", "\t")
	if err != nil {
		return err
	}

	b = append(b, byte('\n'))

	if err := os.WriteFile(tmpPath, b, 0644); err != nil {
		return err
	}

	return os.Rename(tmpPath, fnlPath)
}

func (d *Driver) Read(collection, resource string, v interface{}) error {
	if collection == "" {
		return fmt.Errorf("Missing collection")
	}
	if resource == "" {
		return fmt.Errorf("Missing resource - unable to save record")
	}

	record := filepath.Join(d.dir, collection, resource)
	if _, err := stat(record); err != nil {
		return err
	}

	b, err := os.ReadFile(record + ".json")
	if err != nil {
		return err
	}

	return json.Unmarshal(b, &v)
}

func (d *Driver) ReadAll(collection string) ([]string, error) {
	if collection == "" {
		return nil, fmt.Errorf("Missing collection")
	}

	dir := filepath.Join(d.dir, collection)
	if _, err := stat(dir); err != nil {
		return nil, err
	}

	files, _ := os.ReadDir(dir)
	var ans []string
	for _, c := range files {
		b, err := os.ReadFile(filepath.Join(dir, c.Name()))
		if err != nil {
			return nil, err
		}
		ans = append(ans, string(b))
	}
	return ans, nil
}

func (d *Driver) Delete(collection, resource string) error {

	dir := filepath.Join(d.dir, collection)
	mutex := d.getOrCreateMutex(collection)
	mutex.Lock()
	defer mutex.Unlock()

	switch fi, err := stat(dir); {
	case fi == nil && err != nil:
		{
			return err
		}
	case fi.Mode().IsDir():
		{
			return os.Remove(dir)
		}
	case fi.Mode().IsRegular():
		{
			return os.Remove(dir + ".json")
		}
	}
	return nil
}

func (d *Driver) getOrCreateMutex(collection string) *sync.Mutex {
	m, ok := d.mutexex[collection]
	if !ok {
		m = &sync.Mutex{}
		d.mutexex[collection] = m
	}
	return d.mutexex[collection]
}

func stat(path string) (fi os.FileInfo, err error) {
	if fi, err = os.Stat(path); os.IsNotExist(err) {
		fi, err = os.Stat(path + ".json")
	}
	return
}

func main() {
	dir := "dir1"

	db, err := New(dir, nil)
	if err != nil {
		log.Error(err.Error())
		return
	}

	employees := []User{
		{"John", "23", "23344333", Address{"bangalore", "karnataka", "india", "410013"}},
		{"Paul", "25", "23344333", Address{"san francisco", "california", "USA", "410013"}},
		{"Robert", "27", "23344333", Address{"bangalore", "karnataka", "india", "410013"}},
		{"Vince", "29", "23344333", Address{"bangalore", "karnataka", "india", "410013"}},
		{"Neo", "31", "23344333", Address{"bangalore", "karnataka", "india", "410013"}},
		{"Albert", "32", "23344333", Address{"bangalore", "karnataka", "india", "410013"}},
	}

	for _, user := range employees {
		db.Write("users", user.Name, User{
			Name:    user.Name,
			Age:     user.Age,
			Contact: user.Contact,
			Address: user.Address,
		})
	}

	records, err := db.ReadAll("users")
	if err != nil {
		log.Error(err)
		return
	}

	allusers := []User{}
	fmt.Println(records)

	for _, f := range records {
		employee := User{}
		if err := json.Unmarshal([]byte(f), &employee); err != nil {
			log.Error(err)
			return
		}
		allusers = append(allusers, employee)
	}

	log.Info(allusers)

}
