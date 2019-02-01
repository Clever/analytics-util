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
	String    FieldType = "string"
	Integer   FieldType = "integer"
	Boolean   FieldType = "boolean"
	Timestamp FieldType = "timestamp"
)

// S3MetaData represents all the information we want to add to an analytics object for future reference
// See User-Defined metadata in:
// https://docs.aws.amazon.com/AmazonS3/latest/dev/UsingMetadata.html#object-metadata
type S3MetaData struct {
	SchemaName    *string `json:"x-amz-meta-schema-name"`
	TableName     *string `json:"x-amz-meta-table-name"`
	FieldNames    *string `json:"x-amz-meta-field-names"`
	FieldTypes    *string `json:"x-amz-meta-field-types"`
	fieldNamesArr []string
	fieldTypesArr []FieldType
}

// NewMetadata ...
func NewMetadata(schema, table string, fieldNames []string, fieldTypes []FieldType) *S3MetaData {
	return &S3MetaData{
		SchemaName:    &schema,
		TableName:     &table,
		fieldNamesArr: fieldNames,
		fieldTypesArr: fieldTypes,
	}
}

// NewMetadataFromMap returns a metadata object constructed from the S3 sdk
func NewMetadataFromMap(metadata map[string]*string) (*S3MetaData, error) {
	b, err := json.Marshal(metadata)
	if err != nil {
		return nil, err
	}
	var m S3MetaData
	if err := json.Unmarshal(b, &m); err != nil {
		return nil, err
	}
	m.buildFieldNames()
	m.buildFieldTypes()
	if err := m.validate(); err != nil {
		return nil, err
	}

	return &m, nil
}

// ToMap converts the S3MetaData to the type expected by the S3 sdk
func (m *S3MetaData) ToMap() (map[string]*string, error) {
	m.fieldNamesToString()
	m.fieldTypesToString()
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
	if m.FieldNames == nil || len(m.fieldNamesArr) == 0 {
		return fmt.Errorf(invalidFieldErrorTemplate, "field types")
	}
	if m.FieldTypes == nil || len(m.fieldTypesArr) == 0 {
		return fmt.Errorf(invalidFieldErrorTemplate, "field types")
	}

	if len(strings.Split(*m.FieldNames, comma)) != len(strings.Split(*m.FieldTypes, comma)) {
		return fmt.Errorf("field configuration mismatch. names: %s, types: %s", *m.FieldNames, *m.FieldTypes)
	}

	return nil
}

// []string -> *string
func (m *S3MetaData) fieldNamesToString() {
	s := strings.Join(m.fieldNamesArr, comma)
	m.FieldNames = &s
}

// []FieldTypes -> *string
func (m *S3MetaData) fieldTypesToString() {
	strTypes := make([]string, len(m.fieldTypesArr))
	for idx, elem := range m.fieldTypesArr {
		strTypes[idx] = string(elem)
	}
	s := strings.Join(strTypes, comma)
	m.FieldTypes = &s
}

// *string -> []string
func (m *S3MetaData) buildFieldNames() {
	if m.FieldNames == nil {
		return
	}
	m.fieldNamesArr = strings.Split(*m.FieldNames, comma)
}

// *string -> []FieldTypes
func (m *S3MetaData) buildFieldTypes() {
	if m.FieldTypes == nil {
		return
	}

	types := strings.Split(*m.FieldTypes, comma)
	for _, t := range types {
		m.fieldTypesArr = append(m.fieldTypesArr, FieldType(t))
	}
}
