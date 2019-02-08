# Metadata

A library to assist with tagging analytics data with metadata

## S3 Meta Data

This metadata is used to identify the destination and contents of a data dump.

### Definition
| Name        | AWS metadata field name  | Required | Description |
|-------------|--------------------------|----------|-------------|
| Schema Name | `x-amz-meta-schema-name` | `true`   | the destination schema |
| Table Name  | `x-amz-meta-table-name`  | `true`   | the destination table name |
| Field Names | `x-amz-meta-field-names` | `true`   | the names of the fields included in the data dump |
| Field Types | `x-amz-meta-field-types` | `true`   | the types of the fields included in the data dump (in the same order as `field-names`). See supported types in: `FieldType` |

### Examples

***Simple golang case:***

```go
import (
	"os"

	"github.com/Clever/analytics-util/metadata"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/endpoints"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
)

func upload(bucket, filename string) error {
	s3API := s3.New(session.New(&aws.Config{Region: aws.String(endpoints.UsWest2RegionID)}))
	f, err := os.Open("/tmp/some-file.json")
	if err != nil {
		return err
	}
	defer f.Close()

	schemaName := "your_schema"
	tableName := "your_table"

	m, err := metadata.GenerateS3MetaData(schemaName, tableName, map[string]metadata.FieldType{
		"foo_id":    metadata.String,
		"bar_count": metadata.Integer,
	})
	if err != nil {
		return err
	}

	_, err = s3API.PutObject(&s3.PutObjectInput{
		Body:     f,
		Bucket:   aws.String(bucket),
		Key:      aws.String(filename),
		Metadata: m,
	})
	return err
}
```

***What if you are writing without `Go`?***

Please tag the S3 object with all `required` metadata fields defined above. Please make sure to use the *exact* aws name
