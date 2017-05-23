package bigquery

import (
	"context"
	"strings"

	"google.golang.org/api/iterator"
	"google.golang.org/api/option"

	"fmt"

	"cloud.google.com/go/bigquery"
)

type Schema interface {
	String() string
	GetByFieldName(string) interface{}
	FieldList() []string
}

type BigQuery struct {
	Client *bigquery.Client
}

func NewBigQuery(prjID, conf string) (*BigQuery, error) {
	client, err := bigquery.NewClient(context.Background(),
		prjID, option.WithServiceAccountFile(conf))
	if err != nil {
		return nil, err
	}
	return &BigQuery{
		Client: client,
	}, nil
}

func (bq *BigQuery) run(ctx context.Context, sql string, isStandardSQL bool) (*bigquery.RowIterator, error) {
	q := bq.Client.Query(sql)
	q.QueryConfig.UseStandardSQL = isStandardSQL
	iter, err := q.Read(context.Background())
	return iter, err
}

func (bq *BigQuery) Each(sql string, isStandardSQL bool) chan []interface{} {

	ch := make(chan []interface{})

	iter, err := bq.run(context.Background(), sql, isStandardSQL)
	if err != nil {
		fmt.Println(err)
		return nil
	}

	go func() {
		for {
			var value []bigquery.Value

			err := iter.Next(&value)
			if err == iterator.Done {
				break
			}
			if err != nil {
				if err.Error() == "bigquery: NULL values cannot be read into structs" {
					// ignore
				} else {
					fmt.Println(err)
					return
				}
			}

			row := make([]interface{}, len(value))
			for i, col := range value {
				row[i] = col
			}
			ch <- row
		}
		close(ch)
	}()
	return ch
}

func (bq *BigQuery) SchemaEach(sql string, isStandardSQL bool, schema Schema) chan Schema {
	schemaCh := make(chan Schema)

	iter, err := bq.run(context.Background(), sql, isStandardSQL)
	if err != nil {
		fmt.Println(err)
		return nil
	}

	go func(sc Schema) {
		for {
			err := iter.Next(sc)
			if err == iterator.Done {
				break
			}
			if err != nil {
				if err.Error() == "bigquery: NULL values cannot be read into structs" {
					// ignore
				} else {
					fmt.Println(err)
					return
				}
			}
			schemaCh <- sc
		}
		close(schemaCh)
	}(schema)
	return schemaCh
}

func (bq *BigQuery) Join(row []interface{}, sep string) string {
	s := make([]string, len(row))
	for i := 0; i < len(row); i++ {
		s[i] = fmt.Sprintf("%v", row[i])
	}
	return strings.Join(s, sep)
}
