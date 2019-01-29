package main

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"strconv"
	"time"

	_ "github.com/nakagami/firebirdsql"
	pb "gopkg.in/cheggaaa/pb.v1"
)

var conf config

type (
	//Структа с описанием конфигурации программы.
	config struct {
		Frontol4 struct {
			DBPath              string `json:"db_path"`
			CardTypeGC3000      int    `json:"card_type_gc3000"`
			CardTypeGC5000      int    `json:"card_type_gc5000"`
			CounterTypeDiscount int    `json:"counter_type_discount"`
		} `json:"frontol4"`
		Frontol6 struct {
			DBPath              string `json:"db_path"`
			GiftCardType3000    int    `json:"gift_card_type3000"`
			GiftCardType5000    int    `json:"gift_card_type5000"`
			CounterTypeDiscount int    `json:"counter_type_discount"`
		} `json:"frontol6"`
	}
	//Структура для подарочных карт Frontol 4.
	giftCard4 struct {
		Value   int
		Counter time.Time
		Status  int
	}
	//Структура для накоплений клиентов Frontol 4.
	discountCard4 struct {
		ClientID int
		Counter  int
	}
	//Структура для подарочных карт Frontol 6.
	giftCard6 struct {
		Code         int
		Name         int
		StartBalance float64
		TypeCard     int
		State        int
		StartDate    time.Time
		EndDate      time.Time
	}
)

func main() {
	var n int

	readConfig()

	n = 0
	dbFrontol4, err := sql.Open("firebirdsql", "sysdba:masterkey@"+conf.Frontol4.DBPath)
	if err != nil {
		panic(err)
	}
	defer dbFrontol4.Close()
	dbFrontol4.QueryRow("SELECT Count(*) FROM rdb$relations").Scan(&n)
	if n == 0 {
		panic("Can't connect to DB Frontol4")
	}
	log.Println("Успешное подключение базе данных Frontol 4.")

	n = 0
	dbFrontol6, err := sql.Open("firebirdsql", "sysdba:masterkey@"+conf.Frontol6.DBPath)
	if err != nil {
		panic(err)
	}
	defer dbFrontol6.Close()
	dbFrontol6.QueryRow("SELECT Count(*) FROM rdb$relations").Scan(&n)
	if n == 0 {
		panic("Can't connect to DB Frontol 6")
	}
	log.Println("Успешное подключение базе данных Frontol 6.")
	fmt.Println("")

	gcs4 := getGiftcardsFromFrontol4(dbFrontol4)
	dcs := getClientsFromFrontol4(dbFrontol4)
	gcs6 := prepareGiftcardsForFrontol6(dbFrontol6, gcs4)
	appendGiftcardsToFrontol6(dbFrontol6, gcs6)
	appendDiscountCardsToFrontol6(dbFrontol6, dcs)

}

/*
Функция для получения информации о подарочных картах из Frontol 4.
Функция возвращяет массив объектов giftCard4.
*/
func getGiftcardsFromFrontol4(conn *sql.DB) []giftCard4 {
	var query string

	var gcs4 []giftCard4

	query = `SELECT card.val, SUM(counterd.delta) FROM CCARD as card
	INNER JOIN grpccard as cardt ON cardt.id = card.grpccardid
	INNER JOIN ccardcounter as cc ON cc.ccardid = card.id
	INNER JOIN counter as counter ON counter.id = cc.counterid
	LEFT JOIN counterd as counterd on counter.id = counterd.counterid
	WHERE cardt.code IN (?,?)
	
	GROUP by  card.val	
	`
	start := time.Now()
	rows, err := conn.Query(query, conf.Frontol4.CardTypeGC3000, conf.Frontol4.CardTypeGC5000)
	if err != nil {
		panic(err)
	}
	defer rows.Close()
	for rows.Next() {
		var counter float32
		var gc4 giftCard4

		rows.Scan(&gc4.Value, &counter)

		s := fmt.Sprintf("%.0f", counter)
		if s == "0" {
			gc4.Status = 0
		} else if s == "999" || s == "1999" || s == "1998" {
			gc4.Status = 2
		} else {
			year := s[0:4]
			month := s[4:6]
			day := s[6:8]
			t, _ := time.Parse(time.RFC3339, year+"-"+month+"-"+day+"T00:00:00Z")
			gc4.Status = 1
			gc4.Counter = t
		}
		gcs4 = append(gcs4, gc4)
	}
	elapsed := time.Since(start)

	log.Println("Успешно выгруженно ", len(gcs4), " подарочных карт из Frontol 4.")
	log.Printf("Времени затрачено %s", elapsed)
	fmt.Println("")
	return gcs4
}

/*
Функция конвертирует подарочные карты в формат Frontol 6.
Функция возвращает массив объектов giftCard6.
*/
func prepareGiftcardsForFrontol6(conn *sql.DB, gcs4 []giftCard4) []giftCard6 {
	var code int
	var name int
	var state int
	var endDate time.Time
	var gc6 giftCard6
	var gcs6 []giftCard6

	number5000 := 9998765000000
	number3000 := 9998763000000

	start := time.Now()
	i := 1
	for number3000 < 9998763010000 {
		gc4 := findElementByValue(gcs4, number3000)

		if gc4.Value == 0 {
			code = i
			name = number3000
			state = 0
		} else {
			code = i
			name = gc4.Value
			state = gc4.Status
			if gc4.Status == 1 {
				endDate = gc4.Counter
			}
		}
		gc6.Code = code
		gc6.Name = name
		gc6.State = state
		gc6.EndDate = endDate
		gc6.TypeCard = conf.Frontol6.GiftCardType3000
		gc6.StartBalance = 3000
		gcs6 = append(gcs6, gc6)
		number3000++
		i++
	}
	for number5000 < 9998765010000 {
		gc4 := findElementByValue(gcs4, number5000)

		if gc4.Value == 0 {
			code = i
			name = number5000
			state = 0
		} else {
			code = i
			name = gc4.Value
			state = gc4.Status

			if gc4.Status == 1 {
				endDate = gc4.Counter
			}
		}
		gc6.Code = code
		gc6.Name = name
		gc6.State = state
		gc6.EndDate = endDate
		gc6.TypeCard = conf.Frontol6.GiftCardType5000
		gc6.StartBalance = 5000
		gcs6 = append(gcs6, gc6)
		number5000++
		i++
	}

	elapsed := time.Since(start)
	log.Println("Подготовлено ", len(gcs6), " подарочных карт к загрузке в Frontol 6.")
	log.Printf("Времени затрачено %s", elapsed)
	fmt.Println("")

	return gcs6
}

/*
Функция для загрузки поддарочных карт в Frontol6.
*/
func appendGiftcardsToFrontol6(conn *sql.DB, gcs []giftCard6) {
	var query string

	query = `execute procedure appendgiftcard (?, ?, ?, ?, ?, null,?,0,0,null)`

	start := time.Now()
	log.Println("Начало загрузки подарочных карт в Frontol 6.")
	bar := pb.StartNew(len(gcs))
	for _, gc := range gcs {
		if gc.State == 1 {
			_, err := conn.Exec(query, gc.Code, strconv.Itoa(gc.Name), gc.StartBalance, gc.TypeCard, gc.State, gc.EndDate.Format("2006-01-02"))
			if err != nil {
				panic(err)
			}
		} else {
			_, err := conn.Exec(query, gc.Code, strconv.Itoa(gc.Name), gc.StartBalance, gc.TypeCard, gc.State, nil)
			if err != nil {
				panic(err)
			}
		}
		bar.Increment()

	}
	bar.Finish()
	elapsed := time.Since(start)
	log.Println("Загружено ", len(gcs), " подарочных карт в Frontol 6.")
	log.Printf("Времени затрачено %s", elapsed)
	fmt.Println("")
}

/*
Функция для получения информации о накоплениях клиентов из Frontol 4.
Функция возвращяет массив объектов discountCard4.
*/
func getClientsFromFrontol4(conn *sql.DB) []discountCard4 {
	var query string
	var dcs []discountCard4

	query = `SELECT  cl.id, SUM(cntd.delta) FROM counter as cnt
	INNER JOIN countertype as ct ON ct.id = cnt.countertypeid
	INNER JOIN clientcounter as cl_cnt ON cl_cnt.counterid = cnt.id
	INNER JOIN client as cl ON cl.id = cl_cnt.clientid
	LEFT JOIN counterd as cntd on cnt.id = cntd.counterid
	WHERE  ct.code = ?
	GROUP by cl.id`

	start := time.Now()
	rows, err := conn.Query(query, conf.Frontol4.CounterTypeDiscount)
	if err != nil {
		panic(err)
	}
	defer rows.Close()

	for rows.Next() {
		var counter int
		var clientID int
		var dc discountCard4

		rows.Scan(&clientID, &counter)
		dc.ClientID = clientID
		dc.Counter = counter

		dcs = append(dcs, dc)

	}
	elapsed := time.Since(start)
	log.Println("Успешно выгруженно ", len(dcs), " клиентов из Frontol 4.")
	log.Printf("Времени затрачено %s", elapsed)
	fmt.Println("")

	return dcs

}

/*
Функция для загрузки накоплений по дисконтным картам в Frontol6.
*/
func appendDiscountCardsToFrontol6(conn *sql.DB, dcs []discountCard4) {
	var query string

	query = `execute procedure appendcounterbytype (?, ?, null, ?, null, null)`

	start := time.Now()
	log.Println("Начало загрузки накоплений по дисконтным картам в Frontol 6.")
	bar := pb.StartNew(len(dcs))
	for _, dc := range dcs {
		_, err := conn.Exec(query, conf.Frontol6.CounterTypeDiscount, dc.Counter, dc.ClientID)
		if err != nil {
			panic(err)
		}

		bar.Increment()

	}
	bar.Finish()
	elapsed := time.Since(start)
	log.Println("Загружено ", len(dcs), "  дисконтных карт в Frontol 6.")
	log.Printf("Времени затрачено %s", elapsed)
	fmt.Println("")

}

//Фунция для поиска нужного объекта в массиве giftCard4, по номеру карты
func findElementByValue(gcs []giftCard4, value int) giftCard4 {
	for _, gc := range gcs {
		if gc.Value == value {
			return gc
		}
	}
	return giftCard4{}
}

//Чтение и загрузка конфига в глобальную переменную
func readConfig() {

	jsonFile, err := os.Open("config.json")
	if err != nil {
		panic(err)
	}
	log.Println("Конфиг успешно прочитан.")
	defer jsonFile.Close()

	fmt.Println("")

	byteValue, _ := ioutil.ReadAll(jsonFile)
	json.Unmarshal(byteValue, &conf)
}
