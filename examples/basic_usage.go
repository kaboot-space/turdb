package main

import (
	"fmt"
	"log"
	"os"

	"github.com/turdb/tur/pkg/api"
	"github.com/turdb/tur/pkg/keys"
)

func main() {
	// Remove existing test database
	os.Remove("test.turdb")

	// Create new tur database
	config := api.TurConfig{
		Path:     "test.turdb",
		PageSize: 4096,
	}

	tur, err := api.OpenTur(config)
	if err != nil {
		log.Fatalf("Failed to open tur: %v", err)
	}
	defer tur.Close()

	fmt.Println("Created new tur database")

	// Test TypedLink functionality
	testTypedLinks()

	// Create a table
	table, err := tur.CreateTable("users")
	if err != nil {
		log.Fatalf("Failed to create table: %v", err)
	}

	fmt.Println("Created table 'users'")

	// Add columns
	nameCol, err := table.AddColumn("name", keys.TypeString, false)
	if err != nil {
		log.Fatalf("Failed to add name column: %v", err)
	}

	ageCol, err := table.AddColumn("age", keys.TypeInt, true)
	if err != nil {
		log.Fatalf("Failed to add age column: %v", err)
	}

	activeCol, err := table.AddColumn("active", keys.TypeBool, false)
	if err != nil {
		log.Fatalf("Failed to add active column: %v", err)
	}

	// Add a typed link column
	linkCol, err := table.AddColumn("link", keys.TypeTypedLink, true)
	if err != nil {
		log.Fatalf("Failed to add link column: %v", err)
	}

	fmt.Printf("Added columns: name(%v), age(%v), active(%v), link(%v)\n", 
		nameCol, ageCol, activeCol, linkCol)

	// Create objects
	user1, err := table.CreateObject()
	if err != nil {
		log.Fatalf("Failed to create user1: %v", err)
	}

	user2, err := table.CreateObject()
	if err != nil {
		log.Fatalf("Failed to create user2: %v", err)
	}

	fmt.Printf("Created objects: user1(%v), user2(%v)\n", user1.GetKey(), user2.GetKey())

	// Set values
	if err := user1.Set("name", "Alice"); err != nil {
		log.Fatalf("Failed to set user1 name: %v", err)
	}

	if err := user1.Set("age", int64(30)); err != nil {
		log.Fatalf("Failed to set user1 age: %v", err)
	}

	if err := user1.Set("active", true); err != nil {
		log.Fatalf("Failed to set user1 active: %v", err)
	}

	// Create a typed link and set it
	typedLink := keys.NewTypedLink(table.GetKey(), user2.GetKey(), keys.TypeString)
	if err := user1.Set("link", typedLink); err != nil {
		log.Fatalf("Failed to set user1 link: %v", err)
	}

	if err := user2.Set("name", "Bob"); err != nil {
		log.Fatalf("Failed to set user2 name: %v", err)
	}

	if err := user2.Set("active", false); err != nil {
		log.Fatalf("Failed to set user2 active: %v", err)
	}
	// user2 age is null (not set)

	fmt.Println("Set values for users")

	// Read values
	name1, err := user1.GetString("name")
	if err != nil {
		log.Fatalf("Failed to get user1 name: %v", err)
	}

	age1, err := user1.GetInt("age")
	if err != nil {
		log.Fatalf("Failed to get user1 age: %v", err)
	}

	active1, err := user1.GetBool("active")
	if err != nil {
		log.Fatalf("Failed to get user1 active: %v", err)
	}

	fmt.Printf("User1: name='%s', age=%d, active=%v\n", name1, age1, active1)

	name2, err := user2.GetString("name")
	if err != nil {
		log.Fatalf("Failed to get user2 name: %v", err)
	}

	isNull, err := user2.IsNull("age")
	if err != nil {
		log.Fatalf("Failed to check user2 age null: %v", err)
	}

	active2, err := user2.GetBool("active")
	if err != nil {
		log.Fatalf("Failed to get user2 active: %v", err)
	}

	fmt.Printf("User2: name='%s', age=null(%v), active=%v\n", name2, isNull, active2)

	// Test B+ tree
	btree, err := tur.CreateBTree("name_index")
	if err != nil {
		log.Fatalf("Failed to create btree: %v", err)
	}

	// Insert some test data
	if err := btree.Insert(1, "Alice"); err != nil {
		log.Fatalf("Failed to insert into btree: %v", err)
	}

	if err := btree.Insert(2, "Bob"); err != nil {
		log.Fatalf("Failed to insert into btree: %v", err)
	}

	if err := btree.Insert(3, "Charlie"); err != nil {
		log.Fatalf("Failed to insert into btree: %v", err)
	}

	fmt.Printf("Inserted %d items into btree\n", btree.Size())

	// Search in btree
	value, found := btree.Search(2)
	if found {
		fmt.Printf("Found key 2: %v\n", value)
	}

	// Sync to disk
	if err := tur.Sync(); err != nil {
		log.Fatalf("Failed to sync: %v", err)
	}

	fmt.Println("Successfully synced to disk")
	fmt.Printf("Final file size: %d bytes\n", tur.GetFileSize())
}

func testTypedLinks() {
	fmt.Println("\n=== Testing TypedLink functionality ===")
	
	// Test TypedLink creation
	tableKey := keys.NewTableKey(1)
	objKey := keys.NewObjKey(100)
	typedLink := keys.NewTypedLink(tableKey, objKey, keys.TypeString)
	
	fmt.Printf("Created TypedLink: %s\n", typedLink.String())
	
	// Test null check
	fmt.Printf("Is null: %v\n", typedLink.IsNull())
	
	// Test encoding/decoding
	encoded := typedLink.Encode()
	fmt.Printf("Encoded size: %d bytes\n", len(encoded))
	
	decoded, err := keys.DecodeTypedLink(encoded)
	if err != nil {
		log.Fatalf("Failed to decode TypedLink: %v", err)
	}
	fmt.Printf("Decoded TypedLink: %s\n", decoded.String())
	
	// Test TypedLinkSet
	linkSet := keys.NewTypedLinkSet()
	
	// Add some links
	link1 := keys.NewTypedLink(keys.NewTableKey(1), keys.NewObjKey(100), keys.TypeString)
	link2 := keys.NewTypedLink(keys.NewTableKey(2), keys.NewObjKey(200), keys.TypeInt)
	link3 := keys.NewTypedLink(keys.NewTableKey(1), keys.NewObjKey(101), keys.TypeString)
	
	linkSet.Add(link1)
	linkSet.Add(link2)
	linkSet.Add(link3)
	
	fmt.Printf("LinkSet size: %d\n", linkSet.Size())
	fmt.Printf("Contains link1: %v\n", linkSet.Contains(link1))
	
	// Test filtering by type
	stringLinks := linkSet.GetLinksByType(keys.TypeString)
	fmt.Printf("String type links: %d\n", len(stringLinks))
	
	// Test encoding/decoding LinkSet
	setEncoded := linkSet.Encode()
	fmt.Printf("LinkSet encoded size: %d bytes\n", len(setEncoded))
	
	decodedSet, err := keys.DecodeTypedLinkSet(setEncoded)
	if err != nil {
		log.Fatalf("Failed to decode TypedLinkSet: %v", err)
	}
	fmt.Printf("Decoded LinkSet size: %d\n", decodedSet.Size())
	
	// Test data type helper functions
	fmt.Printf("TypeLink is link type: %v\n", keys.IsLinkType(keys.TypeLink))
	fmt.Printf("TypeTypedLink is link type: %v\n", keys.IsLinkType(keys.TypeTypedLink))
	fmt.Printf("TypeString is link type: %v\n", keys.IsLinkType(keys.TypeString))
	fmt.Printf("TypeList is collection type: %v\n", keys.IsCollectionType(keys.TypeList))
	
	fmt.Println("=== TypedLink tests completed ===\n")
}
