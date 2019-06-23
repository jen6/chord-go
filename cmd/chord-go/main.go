package main

import (
	"fmt"
	"github.com/jen6/chord-go"
	"github.com/labstack/echo"
	"github.com/labstack/echo/middleware"
	"net/http"
)

const IP = "127.0.0.1"
const PORT = "10000"

func main() {
	node := chord.NewNode(IP, PORT)

	e := echo.New()
	e.Use(middleware.Logger())
	//	e.Use(middleware.Recover())
	fmt.Println("echo setted")

	//get k-v on dht rings
	e.GET("/key/:key", func(c echo.Context) error {
		key := c.Param("key")
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
		data := c.FormValue("data")

		err := node.Set(key, data)
		if err != nil {
			return c.String(http.StatusInternalServerError, err.Error())
		}
		return c.String(http.StatusOK, "")
	})

	//find the sucessor of the key
	e.GET("/successor/:id", func(c echo.Context) error {
		return c.String(http.StatusOK, "Hello, World!")
	})

	//notify to predecessor that this my node is predecessor
	e.POST("/successor/:id", func(c echo.Context) error {
		return c.String(http.StatusOK, "Hello, World!")
	})
	e.Start(IP + ":" + PORT)

	//e.Logger.Fatal(e.Start(IP + ":" + PORT))
}
