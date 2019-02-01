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
)

func TestNewMetadataFromMap(t *testing.T) {
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
				SchemaName:    &testSchema,
				TableName:     &testTable,
				FieldNames:    &testFieldID,
				FieldTypes:    &testFieldType,
				fieldNamesArr: []string{testFieldID},
				fieldTypesArr: []FieldType{String},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := NewMetadataFromMap(tt.args)
			require.Equal(t, tt.want, got)
			require.Equal(t, tt.wantErr, err != nil)
		})
	}
}

func TestS3MetaData_ToMap(t *testing.T) {
	tests := []struct {
		name     string
		metadata *S3MetaData
		want     map[string]*string
		wantErr  bool
	}{
		{
			name:     "working case",
			metadata: NewMetadata(testSchema, testTable, []string{testFieldID}, []FieldType{String}),
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
			got, err := tt.metadata.ToMap()
			require.Equal(t, tt.want, got)
			require.Equal(t, tt.wantErr, err != nil)
		})
	}
}
func TestS3MetaData_validate(t *testing.T) {
	tests := []struct {
		name     string
		metadata *S3MetaData
		wantErr  bool
	}{
		{
			name:     "no schema",
			metadata: &S3MetaData{},
			wantErr:  true,
		},
		{
			name: "no table",
			metadata: &S3MetaData{
				SchemaName: &testSchema,
			},
			wantErr: true,
		},
		{
			name: "no field name",
			metadata: &S3MetaData{
				SchemaName: &testSchema,
				TableName:  &testTable,
			},
			wantErr: true,
		},
		{
			name: "no field name arr",
			metadata: &S3MetaData{
				SchemaName: &testSchema,
				TableName:  &testTable,
				FieldNames: &testFieldID,
			},
			wantErr: true,
		},
		{
			name: "no field type",
			metadata: &S3MetaData{
				SchemaName:    &testSchema,
				TableName:     &testTable,
				FieldNames:    &testFieldID,
				fieldNamesArr: []string{testFieldID},
			},
			wantErr: true,
		},
		{
			name: "no field type arr",
			metadata: &S3MetaData{
				SchemaName:    &testSchema,
				TableName:     &testTable,
				FieldNames:    &testFieldID,
				FieldTypes:    &testFieldType,
				fieldNamesArr: []string{testFieldID},
			},
			wantErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.metadata.validate()
			require.Equal(t, tt.wantErr, err != nil)
		})
	}
}
