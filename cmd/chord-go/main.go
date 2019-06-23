package main

import (
	"flag"
	"fmt"
	"github.com/jen6/chord-go"
	"github.com/labstack/echo"
	"github.com/labstack/echo/middleware"
	"net/http"
)

const DEFAULT_IP = "127.0.0.1"
const DEFAULT_PORT = "10000"

func main() {
	ip := flag.String("ip", "", "address of this instance")
	port := flag.String("port", "", "port of this instance")
	successorIp := flag.String(
		"successor-ip",
		"",
		"address of successor ")
	successorPort := flag.String(
		"successor-port",
		"",
		"port of successor")

	flag.Parse()

	if *ip == "" || *port == "" {
		flag.PrintDefaults()
		return
	}

	node := chord.NewNode(*ip, *port)

	if *successorIp != "" && *successorPort != "" {
		successorNode := chord.CalcNode(*successorIp, *successorPort)
		node.Successor = &successorNode
	}

	e := echo.New()
	e.Use(middleware.Logger())
	//	e.Use(middleware.Recover())
	fmt.Println("echo setted")

	//get k-v on dht rings
	e.GET("/key/:key", func(c echo.Context) error {
		key := c.Param("key")
		if !node.IsSuccessorKey(key) {
			addr := node.GetNearestSuccessorAddr("/key", key)
			return c.Redirect(http.StatusFound, addr)
		}

		value, err := node.Get(key)
		if err != nil {
			//exception for key not found
			if value == key {
				return c.String(http.StatusNotFound, "")
			}
			//exception for timeout... ETC
			return c.String(http.StatusInternalServerError, err.Error())
		}
		fmt.Println(value)

		return c.String(http.StatusOK, "value : "+value)
	})

	//put k-v on dht rings
	e.POST("/key/:key", func(c echo.Context) error {
		key := c.Param("key")
		if !node.IsSuccessorKey(key) {
			addr := node.GetNearestSuccessorAddr("/key", key)
			return c.Redirect(http.StatusFound, addr)
		}

		data := c.FormValue("data")

		err := node.Set(key, data)
		if err != nil {
			return c.String(http.StatusInternalServerError, err.Error())
		}
		return c.String(http.StatusOK, "")
	})

	//find the successor of the key
	e.GET("/successor/:id", func(c echo.Context) error {
		id := c.Param("id")
		if !node.IsSuccessor(id) {
			addr := node.GetNearestSuccessorAddr("/successor", id)
			fmt.Println(addr)
			return c.Redirect(http.StatusFound, addr)
		}
		return c.JSON(http.StatusOK, &node)
	})

	//notify to successor, this node is predecessor
	e.POST("/predecessor/:id", func(c echo.Context) error {
		predNode := new(chord.Node)
		if err := c.Bind(predNode); err != nil {
			return c.String(http.StatusInternalServerError, err.Error())
		}
		moveMap, err := node.SetPredecessor(predNode)
		if err != nil {
			return c.String(http.StatusForbidden, err.Error())
		}

		return c.JSON(http.StatusOK, moveMap)
	})
	e.Start("0.0.0.0:" + *port)

	//e.Logger.Fatal(e.Start(IP + ":" + PORT))
}
