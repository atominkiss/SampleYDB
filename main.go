package main

import (
	"context"
	"fmt"
	"github.com/ydb-platform/ydb-go-sdk/v3"
	"github.com/ydb-platform/ydb-go-sdk/v3/table"
	"github.com/ydb-platform/ydb-go-sdk/v3/table/options"
	"github.com/ydb-platform/ydb-go-sdk/v3/table/types"
	"log"
	"math/rand"
	"path"
	"strings"
	"sync"
	"time"
)

func main() {
	ctx := context.Background()
	dsn := "grpc://localhost:2136/local"
	db, err := ydb.Open(ctx, dsn,
		ydb.WithAnonymousCredentials(),
	)
	if err != nil {
		// обработка ошибки подключения
		log.Printf("error connect to YDB: %s", err)
	}
	defer db.Close(ctx)

	err = createTabel(err, db, ctx)
	if err != nil {
		log.Printf("error createTabel: %s", err)
	}

	log.Printf("success create table")

	var wg sync.WaitGroup
	ctx, cancel := context.WithDeadline(context.Background(), time.Now().Add(1000*time.Millisecond))
	defer cancel()
	for i := 0; i < 1000; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			err := insertRandomDataToDB(err, db, ctx)
			if err != nil {
				err = fmt.Errorf("error insertRandomDataToDB: %s", err)
				log.Printf("ошибка в insertRandomDataToDB: %v", err)
			}
		}()
	}
	//select {}
}

func insertRandomDataToDB(err error, db *ydb.Driver, ctx context.Context) error {
	latency := rand.Uint32() % 501
	time.Sleep(time.Duration(latency) * time.Millisecond)
	releaseDate := time.Now().Second()
	err = db.Table().DoTx( // Do retry operation on errors with best effort
		ctx, // context manages exiting from Do
		func(ctx context.Context, tx table.TransactionActor) (err error) { // retry operation
			res, err := tx.Execute(ctx, `
          PRAGMA TablePathPrefix("/local/");
          DECLARE $seriesID AS Uint64;
          DECLARE $title AS Text;
          DECLARE $seriesInfo AS Text;
          DECLARE $releaseDate AS Datetime;
		  DECLARE $comment AS Text;
          UPSERT INTO series ( series_id, title, series_info, release_date, comment )
          VALUES ( $seriesID, $title, $seriesInfo, $releaseDate, $comment );
        `,
				table.NewQueryParameters(
					table.ValueParam("$seriesID", types.Uint64Value(rand.Uint64())),
					table.ValueParam("$title", types.TextValue(generateRandomAlphabeticalString(10))),
					table.ValueParam("$seriesInfo", types.TextValue(generateRandomAlphabeticalString(10))),
					table.ValueParam("$releaseDate", types.DatetimeValue(uint32(releaseDate))),
					table.ValueParam("$comment", types.TextValue("comment")),
				),
			)
			if err != nil {
				return err
			}
			if err = res.Err(); err != nil {
				return err
			}
			return res.Close()
		}, table.WithIdempotent(),
	)
	//log.Printf("insertRandomDataToDB done")
	return err
}

func createTabel(err error, db *ydb.Driver, ctx context.Context) error {
	err = db.Table().Do(ctx,
		func(ctx context.Context, s table.Session) (err error) {
			return s.CreateTable(ctx, path.Join(db.Name(), "series"),
				options.WithColumn("series_id", types.TypeUint64), // not null column
				options.WithColumn("title", types.Optional(types.TypeText)),
				options.WithColumn("series_info", types.Optional(types.TypeText)),
				options.WithColumn("release_date", types.Optional(types.TypeDatetime)),
				options.WithColumn("comment", types.Optional(types.TypeText)),
				options.WithPrimaryKeyColumn("series_id"),
			)
		},
	)
	return err
}

func generateRandomAlphabeticalString(length int) string {
	rand.Seed(time.Now().UnixNano())
	chars := "abcdefghijklmnopqrstuvwxyz"
	var sb strings.Builder
	for i := 0; i < length; i++ {
		sb.WriteByte(chars[rand.Intn(len(chars))])
	}
	return sb.String()
}
