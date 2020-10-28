package main

import (
    "io"
    "fmt"
    "log"
    "net"

    "github.com/dfarr/kafka-lib/pkg"
    "github.com/dfarr/kafka-lib/pkg/filter"
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

    // Define filters
    filters := map[int16]([]filter.Filter){
        3: []filter.Filter{&filter.BrokerFilter{Port: 10092}},
    }

    for {
        // Decode Request
        req, err := kafkalib.DecodeRequest(conn)
        if err == io.EOF {
            fmt.Println("EOF")
            break
        }
        if err != nil {
            fmt.Println("Decode Request error:", err)
            continue
        }

        fmt.Println("Request recieved")
        fmt.Println(req)
        fmt.Println("Size", req.Size)
        fmt.Println("ApiKey", req.ApiKey)
        fmt.Println("ApiVer", req.ApiVer)
        fmt.Println("Body", req.Body)

        // Apply Filters to Request
        for _, filter := range filters[req.ApiKey] {
            filter.Request(req)
        }

        // Encode Request
        err = req.Encode(broker)
        if err != nil {
            fmt.Println("Encode Request error:", err)
            continue
        }

        // Decode Response
        res, err := kafkalib.DecodeResponse(broker, req.ApiKey, req.ApiVer)
        if err != nil {
            fmt.Println("Decode Response error:", err)
            continue
        }

        fmt.Println("Response recieved")
        fmt.Println(res)
        fmt.Println("Size", res.Size)
        fmt.Println("Body", res.Body)

        // Apply Filters to Response
        for _, filter := range filters[req.ApiKey] {
            filter.Response(res)
        }

        // Encode Response
        err = res.Encode(conn, req.ApiKey, req.ApiVer)
        if err != nil {
            fmt.Println("Encode Response error:", err)
            continue
        }

        fmt.Println("Done.")
    }
}
