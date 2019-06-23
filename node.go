package chord

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"log"
	"net/http"
	"time"
)

type Node struct {
	Ip          string `json:"ip"`
	Port        string `json:"port"`
	Id          uint32 `json:"id"`
	Successor   *Node  `json:"succesor"`
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

func (n Node) GenerateAddress() string {
	return "http://" + n.Ip + ":" + n.Port
}

func (n Node) IsSuccessor(id string) bool {
	if n.Successor == nil {
		return true
	}
	fmt.Println(n.Successor)

	if n.Predecessor == nil {
		return false
	}

	key := ConvertStrHex(id)

	//return if this node is successor
	if n.Predecessor.Id < key && key <= n.Id {
		return true
	}

	return false
}

func (n Node) IsSuccessorKey(key string) bool {
	crc := StrToCRC(key)
	strCrc := ConvertHexStr(crc)
	return n.IsSuccessor(strCrc)
}

func (n Node) GetNearestSuccessorAddr(path, id string) string {
	//	key := ConvertStrHex(id)
	addr := n.Successor.GenerateAddress()
	addr += path + "/"
	addr += id
	return addr
}

func (n *Node) findKVToPredecessor(id uint32) map[string]string {
	buf := make(map[string]string)
	for k, v := range n.storage {
		key := ConvertStrHex(k)
		if key <= id {
			buf[k] = v
			delete(n.storage, k)
		}
	}
	return buf
}

func (n *Node) SetPredecessor(pre *Node) (map[string]string, error) {
	if n.Predecessor == nil {
		if pre.Id < n.Id {
			n.Predecessor = pre
			return n.findKVToPredecessor(pre.Id), nil
		} else {
			return map[string]string{}, errors.New("Not predecessor")
		}
	}

	if n.Predecessor.Id < pre.Id && pre.Id < n.Id {
		n.Predecessor = pre
		return n.findKVToPredecessor(pre.Id), nil
	} else {
		return map[string]string{}, errors.New("Not predecessor")
	}
}

func (n *Node) JoinDHT() {
	if n.Successor == nil {
		return
	}
	addr := n.GetNearestSuccessorAddr(
		"/successor",
		ConvertHexStr(n.Id))

	req, err := http.NewRequest("GET", addr, nil)
	if err != nil {
		log.Fatal(err.Error())
	}

	ctx, _ := context.WithTimeout(
		context.Background(),
		10*time.Second)
	findPredecessorReq := req.WithContext(ctx)

	client := &http.Client{}

	resp, err := client.Do(findPredecessorReq)
	if err != nil {
		log.Fatal(err.Error())
	}

	successor := Node{}
	json.NewDecoder(resp.Body).Decode(&successor)
	resp.Body.Close()
	n.Successor = &successor
	if successor.Successor != nil {
		n.Predecessor = successor.Successor
	}

	addr = n.GetNearestSuccessorAddr(
		"/predecessor",
		ConvertHexStr(n.Id))

	marshaled, _ := json.Marshal(*n)
	req, err = http.NewRequest(
		"POST",
		addr,
		bytes.NewBuffer(marshaled))
	if err != nil {
		log.Fatal(err.Error())
	}

	ctx, _ = context.WithTimeout(
		context.Background(),
		10*time.Second)
	notifySuccessorReq := req.WithContext(ctx)
	notifySuccessorReq.Header.Set("Content-Type", "application/json")

	resp, err = client.Do(notifySuccessorReq)
	fmt.Println(resp.Status)
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

func CalcNode(ip, port string) Node {
	node := Node{Ip: ip, Port: port}
	crcTable := crc32.MakeTable(crc32.IEEE)
	hash := crc32.New(crcTable)
	if _, err := io.WriteString(hash, ip+":"+port); err != nil {
		return Node{}
	}
	node.Id = hash.Sum32()
	return node
}

func NewNode(ip, port string) Node {
	node := CalcNode(ip, port)
	node.storage = make(map[string]string)
	return node
}
