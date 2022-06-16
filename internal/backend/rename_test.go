package backend

import (
	"io"
	"io/ioutil"
	"os"
	"path/filepath"
	"testing"

	"github.com/alexbrainman/goissue34681"
)

func TestRenamingOnWindows(t *testing.T) {
	dir, err := ioutil.TempDir("", "rename-test")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)

	testFile := filepath.Join(dir, "test.txt")
	testFileToRemove := filepath.Join(dir, "test-to-remove.txt")
	testTwoFile := filepath.Join(dir, "test-two.txt")

	err = os.WriteFile(testFile, []byte("hello there"), 0o644)
	if err != nil {
		t.Fatalf("could not write original file, %v", err)
	}
	original, err := goissue34681.Open(testFile)
	if err != nil {
		t.Fatalf("could not open original file, %v", err)
	}
	defer original.Close()
	err = os.WriteFile(testTwoFile, []byte("changed"), 0o644)
	if err != nil {
		t.Fatalf("could not write updated file, %v", err)
	}
	err = os.Rename(testFile, testFileToRemove)
	if err != nil {
		t.Fatalf("could not move original file to temp (remove) file, %v", err)
	}
	err = os.Remove(testFileToRemove)
	if err != nil {
		t.Fatalf("could not remove original (remove) file, %v", err)
	}
	err = os.Rename(testTwoFile, testFile)
	if err != nil {
		t.Fatalf("could not move updated over original file, %v", err)
	}
	updated, err := goissue34681.Open(testFile)
	if err != nil {
		t.Fatalf("could not open updated file, %v", err)
	}
	defer updated.Close()

	originalContents, err := io.ReadAll(original)
	if err != nil {
		t.Fatalf("could not read test.txt (original), %v", err)
	}
	if string(originalContents) != "hello there" {
		t.Fatalf(
			`expected "hello there" when reading file (original), got: %s`,
			string(originalContents))
	}

	updatedContents, err := io.ReadAll(updated)
	if err != nil {
		t.Fatalf("could not read test.txt (updated), %v", err)
	}
	if string(updatedContents) != "changed" {
		t.Fatalf(
			`expected "changed" when reading file (updated), got: %s`,
			string(updatedContents))
	}
}
