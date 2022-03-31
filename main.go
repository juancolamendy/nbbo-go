package main

import (
  "log"
  "net"
  "bufio"
  "strings"
  "strconv"
)

type Row struct {
  Symbol string
  Exch   string
  Bid    int64
  Offer  int64
}

func main() {
  dsCh := createDataSource("199.83.14.77:7777")
  aggCh := aggregate(dsCh)
  calculator(aggCh)
}

func createDataSource(connStr string) chan *Row {
  outCh := make(chan *Row)

  go func(){
    conn, err := net.Dial("tcp", connStr)
    if err != nil {
      log.Fatal("Cannot connect")
    }
    defer conn.Close()

    rdr := bufio.NewReader(conn)
    scanner := bufio.NewScanner(rdr)
    scanner.Split(bufio.ScanLines)

    for scanner.Scan() {
      line := scanner.Text()
      log.Printf("%s", line)
      if line[0] == 'Q' {
        parts := strings.Split(line, "|")
        bid, _ := strconv.ParseInt(parts[3], 10, 64)
        offer, _ := strconv.ParseInt(parts[4], 10, 64)
        row := &Row {
          Symbol: parts[1],
          Exch: parts[2],
          Bid: bid,
          Offer: offer,
        }
        // log.Printf("Row: %+v", row)
        outCh <- row
      }
    }        
  }()

  return outCh
}

func aggregate(inCh chan *Row) chan []*Row {
  outCh := make(chan []*Row)

  go func() {
    // init
    // symbol > exchange > (bid, offer)
    wmap := make(map[string]map[string]*Row)

    // logic
    LOOP1:
    for {
        select {
        case row, ok := <-inCh:
          if (!ok) {		        
              break LOOP1
          }
          // log.Printf("Processing row %v\n", row)
          // update the map
          if _, ok := wmap[row.Symbol]; !ok {
            wmap[row.Symbol] = make(map[string]*Row)
          }
          wmap[row.Symbol][row.Exch] = row

          // get list to calc
          list := make([]*Row, 0)
          for _, r := range wmap[row.Symbol] {
            list = append(list, r)
          } 
          outCh <- list
        }
    }  
  }()

  return outCh
}

func calculator (inCh chan []*Row) {
	LOOP1:
	for {
	    select {
	    case list, ok := <-inCh:
		    if (!ok) {		        
		        break LOOP1
		    }
       
        log.Println("-----")
        for _, row := range list {
          log.Printf("Processing row %v\n", row)
        }
	    }
	}  
}
