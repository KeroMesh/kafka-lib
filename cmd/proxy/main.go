package main

import (
    "io"
    "fmt"
    "log"
    "net"

    "github.com/dfarr/kafka-lib/pkg"
)


func main() {
    var err error
    var client net.Listener

    if client, err = net.Listen("tcp", ":10092"); err != nil {
        fmt.Println("Panic 1")
        log.Panic(err)
    }

    for {
        conn, err := client.Accept()
        if err != nil {
            fmt.Println("Panic 2")
            log.Panic(err)
        }

        fmt.Println("Handle new connnection...")
        go handle(conn)
    }
}

func handle(conn net.Conn) {
    var err error

    // Create connection to broker
    broker, err := net.Dial("tcp", ":9092")
    if err != nil {
        fmt.Println("Panic 3")
        log.Panic(err)
    }

    defer broker.Close()

    for {
        req, err := kafkalib.DecodeRequest(conn)
        if err == io.EOF {
            fmt.Println("EOF")
            break
        }
        if err != nil {
            fmt.Println(err)
            continue
        }

        fmt.Println("Request recieved")
        fmt.Println(req)
        // fmt.Println(req.Size)
        fmt.Println(req.ApiKey)
        fmt.Println(req.ApiVer)

        err = req.Encode(broker)
        if err != nil {
            fmt.Println(err)
            continue
        }

        res, err := kafkalib.DecodeResponse(broker, req.ApiKey, req.ApiVer)
        if err != nil {
            fmt.Println(err)
            continue
        }

        fmt.Println("Response recieved")
        fmt.Println(res)
        fmt.Println(res.Size)

        err = res.Encode(conn, req.ApiKey, req.ApiVer)
        if err != nil {
            fmt.Println(err)
            continue
        }

        fmt.Println("Done.")
    }
}
