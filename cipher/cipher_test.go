package cipher

import (
	"testing"
)

var (
	key     = [32]byte{}
	id      = []byte("testID")
	content = []byte("content")

	testEncrypted = []byte{206, 9, 54, 62, 102, 39, 116, 162, 110, 152, 44, 231, 210, 23, 80, 214, 123, 201, 83, 123, 94, 82, 198, 59, 229, 151, 135, 195, 148, 172, 81, 201, 130, 182, 123, 239, 70, 183, 141, 246, 94, 74, 217, 218, 202, 234, 210}
)

func TestEncrypt(t *testing.T) {
	encryptedContent := Encrypt(key, id, content)
	if encryptedContent == nil {
		t.Fatalf("the response is nil")
	}

	// 47 because 24 for the nonce, 16 for the cipher overhead and 7 for "content"
	if len(encryptedContent) != 47 {
		t.Fatalf("the response must be 47 bytes long")
	}
}

func TestDecrypt(t *testing.T) {
	clearContent, err := Decrypt(key, id, testEncrypted)
	if err != nil {
		t.Fatal(err)
	}
	if clearContent == nil {
		t.Fatalf("the response is nil")
	}

	if len(clearContent) != 7 {
		t.Fatalf("the response must be 7 bytes long")
	}

	_, err = Decrypt(key, id, []byte{})
	if err == nil {
		t.Fatalf("must returns an error")
	}
}
