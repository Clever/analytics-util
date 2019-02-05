package metadata

import (
	"testing"

	"github.com/stretchr/testify/require"
)

const (
	mSchema     = "x-amz-meta-schema-name"
	mTable      = "x-amz-meta-table-name"
	mFieldNames = "x-amz-meta-field-names"
	mFieldTypes = "x-amz-meta-field-types"
)

var (
	testSchema    = "schema"
	testTable     = "table"
	testFieldID   = "id"
	testFieldType = "string"
	testFields    = map[string]FieldType{testFieldID: String}
)

func TestNewS3MetaDataFromMap(t *testing.T) {
	tests := []struct {
		name    string
		args    map[string]*string
		want    *S3MetaData
		wantErr bool
	}{
		{
			name: "working case",
			args: map[string]*string{
				mSchema:     &testSchema,
				mTable:      &testTable,
				mFieldNames: &testFieldID,
				mFieldTypes: &testFieldType,
			},
			want: &S3MetaData{
				SchemaName: &testSchema,
				TableName:  &testTable,
				FieldNames: &testFieldID,
				FieldTypes: &testFieldType,
				Fields:     testFields,
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := NewS3MetaDataFromSDKMap(tt.args)
			require.Equal(t, tt.want, got)
			require.Equal(t, tt.wantErr, err != nil)
		})
	}
}

func TestS3MetaData_ConvertToS3SDKFormat(t *testing.T) {
	type args struct {
		table  string
		schema string
		fields map[string]FieldType
	}
	tests := []struct {
		name    string
		args    args
		want    map[string]*string
		wantErr bool
	}{
		{
			name: "working case",
			args: args{
				table:  testTable,
				schema: testSchema,
				fields: testFields,
			},
			want: map[string]*string{
				mSchema:     &testSchema,
				mTable:      &testTable,
				mFieldNames: &testFieldID,
				mFieldTypes: &testFieldType,
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := GenerateS3MetaData(tt.args.schema, tt.args.table, tt.args.fields)
			require.Equal(t, tt.want, got)
			require.Equal(t, tt.wantErr, err != nil)
		})
	}
}
func TestS3MetaData_validate(t *testing.T) {
	tests := []struct {
		name     string
		metadata *S3MetaData
		wantErr  string
	}{
		{
			name:     "no schema",
			metadata: &S3MetaData{},
			wantErr:  "schema",
		},
		{
			name: "no table",
			metadata: &S3MetaData{
				SchemaName: &testSchema,
			},
			wantErr: "table",
		},
		{
			name: "no field name",
			metadata: &S3MetaData{
				SchemaName: &testSchema,
				TableName:  &testTable,
			},
			wantErr: "field name",
		},
		{
			name: "no field type",
			metadata: &S3MetaData{
				SchemaName: &testSchema,
				TableName:  &testTable,
				FieldNames: &testFieldID,
				Fields:     testFields,
			},
			wantErr: "field type",
		},
		{
			name: "no fields map",
			metadata: &S3MetaData{
				SchemaName: &testSchema,
				TableName:  &testTable,
				FieldNames: &testFieldID,
				FieldTypes: &testFieldType,
			},
			wantErr: "field names",
		},
		{
			name: "field names and field types mismatch",
			metadata: &S3MetaData{
				SchemaName: &testSchema,
				TableName:  &testTable,
				FieldNames: testStrPtr("id,doesnt_exist"),
				FieldTypes: &testFieldType,
				Fields:     testFields,
			},
			wantErr: "mismatch",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.metadata.validate()
			require.Contains(t, err.Error(), tt.wantErr)
		})
	}
}

func testStrPtr(s string) *string {
	return &s
}
