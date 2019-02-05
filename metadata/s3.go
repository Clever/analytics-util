package metadata

import (
	"encoding/json"
	"fmt"
	"strings"
)

const (
	invalidFieldErrorTemplate = "invalid metadata data config: %s is empty"
	comma                     = ","
)

// FieldType represents the currently supported types
type FieldType string

// Currently supported field types
const (
	Boolean   FieldType = "boolean"
	Integer   FieldType = "integer"
	MongoID   FieldType = "mongo_id"
	String    FieldType = "string"
	Timestamp FieldType = "timestamp"
)

// S3MetaData represents all the information we want to add to an analytics object for future reference
// See User-Defined metadata in:
// https://docs.aws.amazon.com/AmazonS3/latest/dev/UsingMetaData.html#object-metadata
type S3MetaData struct {
	SchemaName *string `json:"x-amz-meta-schema-name"`
	TableName  *string `json:"x-amz-meta-table-name"`
	FieldNames *string `json:"x-amz-meta-field-names"`
	FieldTypes *string `json:"x-amz-meta-field-types"`
	fields     map[string]FieldType
}

func newS3MetaData(schema, table string, fields map[string]FieldType) *S3MetaData {
	return &S3MetaData{
		SchemaName: &schema,
		TableName:  &table,
		fields:     fields,
	}
}

// GenerateS3MetaData returns a metadata object for use by the S3 sdk
func GenerateS3MetaData(schema, table string, fields map[string]FieldType) (map[string]*string, error) {
	s := newS3MetaData(schema, table, fields)
	return s.ConvertToS3SDKFormat()
}

// NewS3MetaDataFromSDKMap returns a metadata object constructed from the S3 sdk
func NewS3MetaDataFromSDKMap(metadata map[string]*string) (*S3MetaData, error) {
	b, err := json.Marshal(metadata)
	if err != nil {
		return nil, err
	}
	var m S3MetaData
	if err := json.Unmarshal(b, &m); err != nil {
		return nil, err
	}
	m.buildFields()
	if err := m.validate(); err != nil {
		return nil, err
	}

	return &m, nil
}

// ConvertToS3SDKFormat converts the S3MetaData to the map expected by the S3 sdk
func (m *S3MetaData) ConvertToS3SDKFormat() (map[string]*string, error) {
	m.fieldsToStrings()
	if err := m.validate(); err != nil {
		return nil, err
	}
	b, err := json.Marshal(m)
	if err != nil {
		return nil, err
	}
	s := make(map[string]*string)
	if err := json.Unmarshal(b, &s); err != nil {
		return nil, err
	}

	return s, nil
}

// validate determines if we have a valid metadata configuration
func (m *S3MetaData) validate() error {
	if m.SchemaName == nil {
		return fmt.Errorf(invalidFieldErrorTemplate, "schema")
	}
	if m.TableName == nil {
		return fmt.Errorf(invalidFieldErrorTemplate, "table name")
	}
	if m.FieldNames == nil || len(m.fields) == 0 {
		return fmt.Errorf(invalidFieldErrorTemplate, "field names")
	}
	if m.FieldTypes == nil {
		return fmt.Errorf(invalidFieldErrorTemplate, "field types")
	}

	if len(strings.Split(*m.FieldNames, comma)) != len(strings.Split(*m.FieldTypes, comma)) {
		return fmt.Errorf("field configuration mismatch. names: %s, types: %s", *m.FieldNames, *m.FieldTypes)
	}

	return nil
}

func (m *S3MetaData) fieldsToStrings() {
	var fieldNames []string
	var fieldTypes []string

	for k, v := range m.fields {
		fieldNames = append(fieldNames, k)
		fieldTypes = append(fieldTypes, string(v))
	}

	names := strings.Join(fieldNames, comma)
	m.FieldNames = &names

	types := strings.Join(fieldTypes, comma)
	m.FieldTypes = &types
}

func (m *S3MetaData) buildFields() {
	if m.FieldNames == nil || m.FieldTypes == nil {
		return
	}
	fieldNamesArr := strings.Split(*m.FieldNames, comma)
	fieldTypesArr := strings.Split(*m.FieldTypes, comma)
	m.fields = make(map[string]FieldType)

	for idx, fieldName := range fieldNamesArr {
		m.fields[fieldName] = FieldType(fieldTypesArr[idx])
	}
}
