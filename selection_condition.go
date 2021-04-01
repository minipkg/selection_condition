package selection_condition

import (
	"fmt"
	"reflect"
	"sort"
	"strconv"
	"strings"

	"github.com/pkg/errors"

	validation "github.com/go-ozzo/ozzo-validation/v4"
)

const (
	SortOrderParamName = "sort_order"
	SortOrderAsc       = "asc"
	SortOrderDesc      = "desc"

	ConditionSeparator = "__"
	ValuesSeparator    = ","

	ConditionEq  = "eq"
	ConditionGt  = "gt"
	ConditionGte = "gte"
	ConditionLt  = "lt"
	ConditionLte = "lte"
	ConditionIn  = "in"
	ConditionBt  = "bt"

	DefaultWhereCondition = ConditionEq
	DefaultSortDirect     = SortOrderAsc
)

var SortOrderVariants = []interface{}{"", SortOrderAsc, SortOrderDesc}

var ConditionVariants = []interface{}{
	ConditionEq,
	ConditionGt,
	ConditionGte,
	ConditionLt,
	ConditionLte,
	ConditionIn,
	ConditionBt,
}

type SelectionCondition struct {
	Where     interface{}
	SortOrder []map[string]string
	Limit     uint
	Offset    uint
}

func (e SelectionCondition) Validate() error {
	return validation.ValidateStruct(&e) //validation.Field(&e.SortOrder, validation.Each(validation.In(SortOrderVariants...))),

}

type WhereCondition struct {
	Field     string
	Condition string
	Value     interface{}
}

type WhereConditions []WhereCondition

func (s WhereCondition) Validate() error {
	return validation.ValidateStruct(&s,
		validation.Field(&s.Condition, validation.In(ConditionVariants...)),
		validation.Field(&s.Value, validation.When(s.Condition == ConditionBt, validation.Length(2, 2))),
	)
}

func (s WhereConditions) Validate() error {
	return validation.Validate([]WhereCondition(s))
}

func ParseQueryParams(params map[string][]string, struc interface{}) (*SelectionCondition, error) {
	structType, err := getTypeOfAStruct(struc)
	if err != nil {
		return nil, err
	}

	conditions := SelectionCondition{}
	whereConditions := make(WhereConditions, 0, len(params))
	indexesByNames := structFieldIndexesByJsonName(structType)

	for key, vals := range params {
		if len(vals) < 0 {
			continue
		}

		sortOrderConditions, ok, err := parseSortOrderParam(structType, indexesByNames, key, vals)
		if err != nil {
			return nil, err
		}
		if ok {
			conditions.SortOrder = sortOrderConditions
			continue
		}

		whereCondition, ok, err := parseWhereParam(structType, indexesByNames, key, vals)
		if err != nil {
			return nil, err
		}
		if !ok {
			continue
		}
		whereConditions = append(whereConditions, *whereCondition)
	}
	conditions.Where = whereConditions
	return &conditions, nil
}

func parseWhereParam(structType reflect.Type, indexesByNames map[string]int, key string, vals []string) (*WhereCondition, bool, error) {
	paramName, strCond, err := splitConditionParameterName(key)
	if err != nil {
		return nil, false, err
	}

	fieldName, fieldKind, ok := getFieldNameAndKindByName(structType, indexesByNames, paramName)
	if !ok {
		return nil, false, nil
	}

	value, err := string2valByCondition(vals[0], strCond, fieldKind)
	if err != nil {
		return nil, false, err
	}

	return &WhereCondition{
		Field:     fieldName,
		Condition: strCond,
		Value:     value,
	}, true, nil
}

func parseSortOrderParam(structType reflect.Type, indexesByNames map[string]int, key string, vals []string) ([]map[string]string, bool, error) {
	if key != SortOrderParamName {
		return nil, false, nil
	}
	params := strings.Split(vals[0], ",")
	sortOrderParams := make([]map[string]string, 0, len(params))

	for _, param := range params {
		paramName, sortDirect, err := splitSortOrderParameterName(param)
		if err != nil {
			return nil, false, err
		}

		fieldName, _, ok := getFieldNameAndKindByName(structType, indexesByNames, paramName)
		if !ok {
			continue
		}
		sortOrderParams = append(sortOrderParams, map[string]string{fieldName: sortDirect})
	}

	return sortOrderParams, true, nil
}

func getTypeOfAStruct(struc interface{}) (reflect.Type, error) {
	stVal := reflect.ValueOf(struc)
	stType := stVal.Type()
	if stType.Kind() != reflect.Ptr {
		return nil, fmt.Errorf("Parameter struc must be a pointer")
	}

	stValElem := stVal.Elem()

	outPtrType := stValElem.Kind()
	if outPtrType != reflect.Struct {
		return nil, fmt.Errorf("Parameter struc must be a pointer on a struct")
	}
	return stValElem.Type(), nil
}

func getFieldNameAndKind(structType reflect.Type, fieldIndex int) (fieldName string, fieldKind reflect.Kind) {
	field := structType.Field(fieldIndex)
	return field.Name, field.Type.Kind()
}

func getFieldNameAndKindByName(structType reflect.Type, indexesByNames map[string]int, paramName string) (fieldName string, fieldKind reflect.Kind, ok bool) {
	fieldIndex, ok := indexesByNames[paramName]
	if !ok {
		return "", fieldKind, false
	}
	fieldName, fieldKind = getFieldNameAndKind(structType, fieldIndex)
	return fieldName, fieldKind, true
}

func string2valByCondition(strValue string, condition string, kind reflect.Kind) (value interface{}, err error) {
	var isSlice bool
	var strValues []string

	if condition == ConditionIn || condition == ConditionBt {
		isSlice = true
		strValues = strings.Split(strValue, ValuesSeparator)
	}

	if isSlice {
		vals := make([]interface{}, 0, len(strValues))

		for _, v := range strValues {
			val, err := string2val(v, kind)
			if err != nil {
				return nil, err
			}
			vals = append(vals, val)
		}
		sliceSort(vals)
		value = vals
	} else {
		value, err = string2val(strValue, kind)
	}
	return value, err
}

func sliceSort(sl []interface{}) {
	sort.Slice(sl, func(i, j int) bool {
		if iEl, ok := sl[i].(string); ok {
			return iEl < sl[j].(string)
		}

		if iEl, ok := sl[i].(uint64); ok {
			return iEl < sl[j].(uint64)
		}

		if iEl, ok := sl[i].(int64); ok {
			return iEl < sl[j].(int64)
		}

		if iEl, ok := sl[i].(bool); ok {
			return !iEl && iEl != sl[j].(bool)
		}

		if iEl, ok := sl[i].(float64); ok {
			return iEl < sl[j].(float64)
		}
		return false
	})
	return
}

func string2val(strValue string, kind reflect.Kind) (value interface{}, err error) {

	switch kind {
	case reflect.Bool:
		value, err = strconv.ParseBool(strValue)
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		value, err = strconv.ParseUint(strValue, 10, 64)
	case reflect.Int:
		value, err = strconv.ParseInt(strValue, 10, 64)
	case reflect.Float32, reflect.Float64:
		value, err = strconv.ParseFloat(strValue, 64)
	case reflect.String:
		value = strValue
	default:
		value = nil
	}
	return value, err
}

func splitConditionParameterName(param string) (field string, condition string, err error) {
	return splitParameterName(param, DefaultWhereCondition, ConditionVariants)
}

func splitSortOrderParameterName(param string) (field string, sortOrder string, err error) {
	return splitParameterName(param, DefaultSortDirect, SortOrderVariants)
}

func splitParameterName(param string, defaultCondition string, variants []interface{}) (field string, condition string, err error) {
	if !strings.Contains(param, ConditionSeparator) {
		return param, defaultCondition, nil
	}

	s := strings.Split(param, ConditionSeparator)
	if len(s) != 2 {
		return "", "", errors.Errorf("Must be only one separator %q in name of parameter %s", ConditionSeparator, param)
	}

	field = s[0]
	condition = s[1]
	err = validation.Validate(condition, validation.In(variants...))
	if err != nil {
		return "", "", err
	}

	return field, condition, nil
}

func ParseQueryParamsIntoStruct(params map[string][]string, out interface{}) error {
	v := make(map[string]string, len(params))

	for key, vals := range params {
		if len(vals) > 0 {
			v[key] = vals[0]
		}
	}

	return strings2struct(v, out)
}

func ParseUintParam(param string) (uint, error) {
	if param == "" {
		return 0, errors.New("empty")
	}

	paramVal, err := strconv.ParseUint(param, 10, 64)
	if err != nil {
		return 0, err
	}
	return uint(paramVal), nil
}

func strings2struct(data interface{}, out interface{}) error {
	outVal := reflect.ValueOf(out)
	outType := outVal.Type()

	if outType.Kind() != reflect.Ptr {
		return fmt.Errorf("Parameter out must be a pointer")
	}

	outValElem := outVal.Elem()

	if !outValElem.CanSet() {
		return fmt.Errorf("!outValElem.CanSet()")
	}
	outPtrType := reflect.Indirect(outVal).Kind()
	dataVal := reflect.ValueOf(data)

	switch outPtrType {
	case reflect.Bool:
		str, ok := data.(string)
		if !ok {
			return fmt.Errorf("Data mast be a string")
		}
		paramVal, err := strconv.ParseBool(str)
		if err != nil {
			return err
		}
		outValElem.SetBool(paramVal)
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		str, ok := data.(string)
		if !ok {
			return fmt.Errorf("Data mast be a string")
		}

		paramVal, err := strconv.ParseUint(str, 10, 64)
		if err != nil {
			return err
		}
		outValElem.SetUint(paramVal)
	case reflect.Int:
		str, ok := data.(string)
		if !ok {
			return fmt.Errorf("Data mast be a string")
		}

		paramVal, err := strconv.ParseInt(str, 10, 64)
		if err != nil {
			return err
		}
		outValElem.SetInt(paramVal)
	case reflect.Float32, reflect.Float64:
		str, ok := data.(string)
		if !ok {
			return fmt.Errorf("Data mast be a string")
		}

		paramVal, err := strconv.ParseFloat(str, 64)
		if err != nil {
			return err
		}
		outValElem.SetFloat(paramVal)
	case reflect.String:
		res, ok := data.(string)
		if !ok {
			return fmt.Errorf("Data mast be a string")
		}
		outValElem.SetString(res)
	case reflect.Slice:
		dataKind := dataVal.Kind()
		if dataKind != reflect.Slice {
			return fmt.Errorf("Wrong type of data: %v", dataKind)
		}
		len := dataVal.Len()
		elemType := outType.Elem().Elem()
		slice := reflect.MakeSlice(outValElem.Type(), 0, len)

		for i := 0; i < len; i++ {
			dataElem := dataVal.Index(i)
			elem := reflect.New(elemType)

			err := strings2struct(dataElem.Interface(), elem.Interface())
			if err != nil {
				return err
			}
			e := reflect.Indirect(elem)
			slice = reflect.Append(slice, e)
		}
		outValElem.Set(slice)
	case reflect.Map:
		return fmt.Errorf("Kind Map\n")
	case reflect.Struct:
		dataKind := dataVal.Kind()
		if dataKind != reflect.Map { //	структура после анмаршалинга распознаётся как мапа
			return fmt.Errorf("Wrong type of data: %v", dataKind)
		}
		iter := dataVal.MapRange()

		indexesByNames := structFieldIndexesByJsonName(outValElem.Type())

		for iter.Next() {
			k := iter.Key()
			v := iter.Value()

			i, ok := indexesByNames[k.String()]
			if !ok {
				continue
			}
			field := outValElem.Field(i)

			if !field.CanAddr() {
				return fmt.Errorf("Cannot get address!")
			}
			err := strings2struct(v.Interface(), field.Addr().Interface())
			if err != nil {
				return err
			}
		}
	default:
		return fmt.Errorf("Kind unknown! Kind() = %v ; data = %#v\n", dataVal.Kind(), data)
	}

	return nil
}

func structFieldIndexesByJsonName(struc reflect.Type) map[string]int {
	numField := struc.NumField()
	res := make(map[string]int, numField)

	for i := 0; i < numField; i++ {
		field := struc.Field(i)
		name := field.Tag.Get("json")
		if name == "" {
			name = field.Name
		}
		res[name] = i
	}
	return res
}
