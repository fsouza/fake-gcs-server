package backend

import (
	"io"
	"os"
	"path/filepath"
	"testing"
)

func TestRenamingOnWindows(t *testing.T) {
	testFile := filepath.Join(os.TempDir(), "test.txt")
	testTwoFile := filepath.Join(os.TempDir(), "test-two.txt")

	err := os.WriteFile(testFile, []byte("hello there"), 0o644)
	if err != nil {
		t.Fatalf("could not write original file, %v", err)
	}
	original, err := os.Open(testFile)
	if err != nil {
		t.Fatalf("could not open original file, %v", err)
	}
	defer original.Close()
	err = os.WriteFile(testTwoFile, []byte("changed"), 0o644)
	if err != nil {
		t.Fatalf("could not write updated file, %v", err)
	}
	err = os.Rename(testTwoFile, testFile)
	if err != nil {
		t.Fatalf("could not move updated over original file, %v", err)
	}
	updated, err := os.Open(testFile)
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
