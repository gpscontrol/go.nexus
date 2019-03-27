package main
/*
 CREATE TABLE `logger` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `imei` varchar(16) NOT NULL,
  `time_stamp` datetime DEFAULT NULL,
  `log` text,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB AUTO_INCREMENT=9678 DEFAULT CHARSET=utf8

CREATE TABLE `positions` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `imei` varchar(16) DEFAULT NULL,
  `latitude` decimal(10,6) DEFAULT NULL,
  `longitude` decimal(10,6) DEFAULT NULL,
  `altitude` int(11) DEFAULT NULL,
  `speed` float DEFAULT NULL,
  `azimuth` float DEFAULT NULL,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB AUTO_INCREMENT=3272 DEFAULT CHARSET=utf8
 */
import (
    "bufio"
    "fmt"
    "log"
    "net"
    "strings"
    _ "github.com/go-sql-driver/mysql"
    "database/sql"
    "time"
    "github.com/gorilla/websocket"
    "net/url"
    "flag"
    //"strconv"
    //"os"
    //"encoding/json"
)

type Positions struct {
   id   string
   latitude string
   longitude string
}

type Data struct {
    Name    string
    Positions []Positions
}

type JSONDoc struct {
	NestedArray  []NestedArrayElem `json:"nestedarray"`
}
type NestedArrayElem struct {
	Street string `json:"street"`
        Cell string `json:"cell"`

}
type Address struct {

}
type Phone struct {
	Cell string `json:"cell"`
}
var (
    // DBCon is the connection handle
    // for the database
    db *sql.DB
)
var addr = flag.String("addr", "127.0.0.1:9002", "http service address")



func writeLog(imei string,data string) {
    current := time.Now()
    receptionDate := fmt.Sprintf("%d-%02d-%02d %02d:%02d:%02d", current.Year(), current.Month(), current.Day(), current.Hour(), current.Minute(), current.Second())
    cmd := "INSERT INTO logger (imei, time_stamp,log) VALUES ('" + imei+"','" + receptionDate+"','"+data + "');"
    if db!= nil{
        _, err :=db.Exec(cmd)
        if err != nil {
                //panic(err)
                fmt.Println(err)
        }
    } else {
        fmt.Println("Error db handler lost")
    }
    u := url.URL{Scheme: "ws", Host: *addr, Path: "/"}
    c, _, err := websocket.DefaultDialer.Dial(u.String(), nil)
	if err != nil {
		log.Fatal("dial:", err)
	}
	defer c.Close()

    fields := strings.Split(data,",")
    //fmt.Println("Len fields:",len(fields))
    if len(fields)>1 {
        //imei:=fields[1]
        identifier := fields[2]
        if (identifier == "AAA") {
            fmt.Println("AAA")

            imei,eventcode,latitude,longitude,gps_utc,valid,speed,azimuth,altitude:=fields[1],fields[3],fields[4],fields[5],fields[6],fields[7],fields[10],fields[11],fields[13]
            fmt.Println("Imei:", imei)
            fmt.Println("EC:", eventcode)
            fmt.Println("Latitude:", latitude)
            fmt.Println("Longitude:",longitude)
            year := "20" + gps_utc[0:2]
            month := gps_utc[2:4]
            day := gps_utc[4:6]
            hour := gps_utc[6:8]
            minute := gps_utc[8:10]
            seconds := gps_utc[10:12]
            generationDate := year + "-" + month+ "-"+  day + "T" + hour + ":"+minute +":"+ seconds
            fmt.Println("UTC:",generationDate)
            fmt.Println("valid:", valid)
            fmt.Println("speed:", speed)
            fmt.Println("azimuth:", azimuth)
            fmt.Println("altitude:", altitude)


            /*
            res2B := &JSONDoc{}
            res2B.NestedArray = []NestedArrayElem{
                    {Street:"foo",Cell: "bar"},
                {Street:"foo2",Cell: "bar2"},
            }

            var empty=[]NestedArrayElem{{Street:"foo",Cell: "bar"},}
            res2B.NestedArray = append(res2B.NestedArray,empty)
            res, _ := json.Marshal(res2B)
	    fmt.Println(string(res))
	    */
           /*
            if s, err := strconv.ParseFloat(latitude, 64); err == nil {
                lat=s // 3.1415927410125732
            }
            lon:=0.0
            if l, err := strconv.ParseFloat(longitude, 64); err == nil {
                lon=l // 3.1415927410125732
            }

            var positions []Positions
            bytes,err := json.Marshal(Positions{id: imei, latitude: lat, longitude:lon })
            fmt.Println(string(bytes))
            positions = append(positions, Positions{id: imei, latitude: lat, longitude:lon })
            fmt.Println(len(positions))
            result, _ := json.Marshal(positions)
            fmt.Println(string(result))
            */
            //p:="{\"positions\":[{\"id\":10591,\"attributes\":{\"batteryLevel\":0.0,\"distance\":9.97,\"totalDistance\":107785.59,\"motion\":false},\"deviceId\":1,\"type\":null,\"protocol\":\"osmand\",\"serverTime\":\"2019-03-26T15:34:03.710+0000\",\"deviceTime\":\"2019-03-26T15:34:03.000+0000\",\"fixTime\":\"2019-03-26T15:34:03.000+0000\",\"outdated\":false,\"valid\":true,\"latitude\":19.522701,\"longitude\":-99.238399,\"altitude\":2313.800048828125,\"speed\":0.0,\"course\":90.0,\"address\":null,\"accuracy\":25.711999893188477,\"network\":null}]}"
            //d:="{\"devices\": [{\"id\": 2, \"attributes\": {}, \"groupId\": 0, \"name\": \"357042061712320\", \"uniqueId\": \"357042061712320\",\"status\": \"online\", \"lastUpdate\": \"2019-03-21T00:23:01.872+0000\", \"positionId\": 10242,\"geofenceIds\": [], \"phone\": \"\", \"model\": \"\", \"contact\": \"\", \"category\": null, \"disabled\": false}]}"
            //p:="{\"positions\": [{\"id\": 10248,\"attributes\": {\"event\": 35, \"sat\": 11, \"hdop\": 1.0, \"odometer\": 0,"
            //p=p+"\"runtime\": \"1348\",\"status\": \"0000\", \"adc1\": 0, \"adc2\": 0, \"adc3\": 0, \"battery\": 1706, \"power\": 0,"
            //p=p+"\"distance\": 51.08, \"totalDistance\": 8139.43, \"motion\": true}, \"deviceId\": 2,\"type\": null, \"protocol\": \"meitrack\", \"serverTime\": \"2019-03-21T00:23:01.844+0000\",\"deviceTime\": \"2019-03-21T00:23:01.000+0000\", \"fixTime\": \"2019-03-21T00:23:01.000+0000\",\"outdated\": false, \"valid\": true, \"latitude\": "+lat+", \"longitude\": "
            //p=p+lon+",\"altitude\": 0.0, \"speed\": 40.496775, \"course\": "+azimuth+", \"address\": null, \"accuracy\": 0.0,\"network\": {\"radioType\": \"gsm\", \"considerIp\": false, \"cellTowers\": [{\"cellId\": 0, \"locationAreaCode\": 0, \"mobileCountryCode\": 0, \"mobileNetworkCode\": 0,\"signalStrength\": 14}]}}]}"
            //p:="Hello"
            //p := "{\"positions\": [{\"id\": "+imei+",\"attributes\": {\"event\":"+eventcode+", \"sat\": 11, \"hdop\": 1.0, \"odometer\": 0, \"runtime\": \"1348\",\"status\": \"0000\", \"adc1\": 0, \"adc2\": 0, \"adc3\": 0, \"battery\": 1706, \"power\": 0,\"distance\": 51.08, \"totalDistance\": 8139.43, \"motion\": true}, \"deviceId\": 2,\"type\": null, \"protocol\": \"meitrack\", \"serverTime\": "+generationDate+",\"deviceTime\": "+generationDate+", \"fixTime\": "+generationDate+",\"outdated\": false, \"valid\": "+valid+", \"latitude\": " + latitude + ", \"longitude\": " + longitude + ",\"altitude\": "+altitude+", \"speed\": "+speed+", \"course\": " + azimuth + ", \"address\": null, \"accuracy\": 0.0,\"network\": {\"radioType\": \"gsm\", \"considerIp\": false, \"cellTowers\": [{\"cellId\": 0, \"locationAreaCode\": 0, \"mobileCountryCode\": 0, \"mobileNetworkCode\": 0,\"signalStrength\": 14}]}}]}"

            //d:="{\"devices\": [{\"id\": 2, \"attributes\": {}, \"groupId\": 0, \"name\": \"357042061712320\", \"uniqueId\": \"357042061712320\",\"status\": \"online\", \"lastUpdate\": \"2019-03-21T00:23:01.872+0000\", \"positionId\": 10242,\"geofenceIds\": [], \"phone\": \"\", \"model\": \"\", \"contact\": \"\", \"category\": null, \"disabled\": false}]}"

            cmd := "INSERT INTO positions (imei, latitude,longitude) VALUES ('" + imei+"','" + latitude+"','"+longitude + "') ON DUPLICATE KEY UPDATE latitude="+latitude+",longitude="+longitude+";"
            fmt.Println(cmd)
            if db!= nil{
                _, err :=db.Exec(cmd)
                if err != nil {
                        //panic(err)
                        fmt.Println(err)
                }
            } else {
                fmt.Println("Error db handler lost")
            }
            rows, err := db.Query("select imei,latitude,longitude from positions;")
            var qimei string
            var qlat  string
            var qlon string
            p:="{\"positions\": ["

            for rows.Next() {
                err = rows.Scan(&qimei, &qlat, &qlon)
                //p:="{\"positions\": [{\"id\": "+qimei+",\"attributes\": {\"event\": 35, \"sat\": 11, \"hdop\": 1.0, \"odometer\": 0, \"runtime\": \"1348\",\"status\": \"0000\", \"adc1\": 0, \"adc2\": 0, \"adc3\": 0, \"battery\": 1706, \"power\": 0,\"distance\": 51.08, \"totalDistance\": 8139.43, \"motion\": true}, \"deviceId\": 2,\"type\": null, \"protocol\": \"meitrack\", \"serverTime\": \"2019-03-21T00:23:01.844+0000\",\"deviceTime\": \"2019-03-21T00:23:01.000+0000\", \"fixTime\": \"2019-03-21T00:23:01.000+0000\",\"outdated\": false, \"valid\": true, \"latitude\": "+qlat+", \"longitude\": "+qlon+",\"altitude\": 0.0, \"speed\": 40.496775, \"course\": "+azimuth+", \"address\": null, \"accuracy\": 0.0,\"network\": {\"radioType\": \"gsm\", \"considerIp\": false, \"cellTowers\": [{\"cellId\": 0, \"locationAreaCode\": 0, \"mobileCountryCode\": 0, \"mobileNetworkCode\": 0,\"signalStrength\": 14}]}}]}"
                //p:="{\"positions\": [{\"id\": "+qimei+",\"latitude\": "+qlat+", \"longitude\": "+qlon+",\"altitude\": 0.0, \"speed\": 40.496775, \"course\": "+azimuth+"}]}"
                p=p+"{\"id\": "+qimei+",\"latitude\": "+qlat+", \"longitude\": "+qlon+",\"altitude\": 0.0, \"speed\": 40.496775, \"course\": "+azimuth+"},"


            }
            p = p[0:len(p)-1];

            p=p+"]}"
            fmt.Println(p)
                err = c.WriteMessage(websocket.TextMessage, []byte(p))
                if err != nil {
                    log.Println("write:", err)
                    return
                }
                if err != nil {
                    log.Fatal("dial:", err)
                }
        }
    }
/*
    err = c.WriteMessage(websocket.TextMessage, []byte(d))
			if err != nil {
				log.Println("write:", err)
				return
			}
    if err != nil {
		log.Fatal("dial:", err)
	}
*/
}


func handleConnection(conn net.Conn) {
    defer conn.Close()
    scanner := bufio.NewScanner(conn)
    for scanner.Scan() {
        message := scanner.Text()
        fields := strings.Split(message,",")
        if len(fields)>1 {
	    imei := fields[1]
	    writeLog(imei,message);
        }
        fmt.Println("Message Received:", message)
        newMessage := strings.ToUpper(message)
        conn.Write([]byte(newMessage + "\n"))
    }

    if err := scanner.Err(); err != nil {
        fmt.Println("error:", err)
    }
}

func main() {



    var err error
    db, err = sql.Open("mysql", "gpscontrol:qazwsxedc@/nexus")
    if err != nil {
		panic(err.Error()) // Just for example purpose. You should use proper error handling instead of panic
    }
    ln, err := net.Listen("tcp", "127.0.0.1:8500")
    if err != nil {
        log.Fatal(err)
    }

    fmt.Println("Accept connection on port 8500")

    for {
        conn, err := ln.Accept()
        if err != nil {
            log.Fatal(err)
        }
        fmt.Println("Calling handleConnection")
        go handleConnection(conn)
    }
}

