package chord

import (
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
)

type Node struct {
	Ip          string `json:"ip"`
	Port        string `json:"port"`
	Id          uint32 `json:"id"`
	Succesor    *Node  `json:"succesor"`
	Predecessor *Node  `json:"predecessor"`
	storage     map[string]string
}

func (n Node) Get(key string) (string, error) {
	crc := StrToCRC(key)
	strCrc := ConvertHexStr(crc)
	val, ok := n.storage[strCrc]
	if !ok {
		return key, errors.New("no key in this storage")
	}
	fmt.Printf("[log] key : %s -> %s and value : %s\n",
		key, strCrc, val)
	return val, nil
}

func (n *Node) Set(key, value string) error {
	crc := StrToCRC(key)
	strCrc := ConvertHexStr(crc)
	n.storage[strCrc] = value
	fmt.Printf("[log] key : %s -> %s and value : %s\n",
		key, strCrc, value)
	return nil
}

func ConvertStrHex(id string) uint32 {
	decode, _ := hex.DecodeString(id)
	return binary.LittleEndian.Uint32(decode)
}

func ConvertHexStr(id uint32) string {
	buf := make([]byte, 4)
	binary.LittleEndian.PutUint32(buf, id)
	return hex.EncodeToString(buf)
}

func StrToCRC(str string) uint32 {
	crcTable := crc32.MakeTable(crc32.IEEE)
	hash := crc32.New(crcTable)
	if _, err := io.WriteString(hash, str); err != nil {
		return 0
	}
	return hash.Sum32()
}

func NewNode(ip, port string) Node {
	node := Node{Ip: ip, Port: port}
	node.storage = make(map[string]string)
	crcTable := crc32.MakeTable(crc32.IEEE)
	hash := crc32.New(crcTable)
	if _, err := io.WriteString(hash, ip+":"+port); err != nil {
		return Node{}
	}
	node.Id = hash.Sum32()
	return node
}
