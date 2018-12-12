package bkorm

import (
    "reflect"
    "errors"

    json "github.com/jabbawockeez/go-simplejson"
    . "github.com/jabbawockeez/go-utils"
)


func (o *Orm) Do() (result *json.Json, err error) {
    defer o.reset()

    var pointer interface{}
    var result_data *json.Json

    o.beforeDo()
    //o.Show()

	switch o.conf.GetString("Method") {
	case POST:
		result, err = o.doPost()

        switch o.conf.GetString("Operation") {
        case C:
            pointer = o.conf.Get("C_obj_pointer").Interface()

            result_data, err = o.ajustDataFormat(result.Get("data"))
            o.conf.EnSet(result.GetInt("data", "bk_inst_id"), "U_InstId")
            err = o.updateAsst()
        case R:
            result_data = result.GetPath("data", "info")
            pointer = o.conf.Get("R_obj_pointer").Interface()

            if result_data.Length() == 0 {
                err = errors.New("No object found!")
                //break
                return
            }

            if o.conf.GetBool("R_ReadAsst") {
                result_data, err = o.readAsst(result_data)
            }
            if o.conf.GetBool("R_ReadRef") {
                result_data, err = o.readRef(result_data)
            }

            result_data, err = o.ajustDataFormat(result_data)

        //case U:
        default:
            panic("No operation specified!")
        }

        err = result_data.ToStruct(pointer)
	case DELETE:
		result, err = o.doDelete()
       
        //if result.GetBool("result") {
        //    o.deleteAsst()
        //}
	case PUT:
		result, err = o.doPut()

        //o.conf.EnSet(o.GetInt("bk_inst_id"), "Temp", "InstId")
        err = o.updateAsst()
    default:
        panic("http method not specified!")
	}

    return
}

func (o *Orm) ajustDataFormat(data *json.Json) (result_data *json.Json, err error) {
    /*
    One convenient way to unmarshal a json.Json object to struct pointer
    is to use Json.ToStruct method. But before we can unmarshal it,
    we have to deal with the foreign key fields in the response returned 
    by bk cmdb. 
    In a nutshell, if the Operation is C(create), the foreign key in the 
    response is an empty string, we should remove it, ortherwise unmarshal 
    would be failed, and if the Operation is R(read), both toOne and toMany 
    fields would be returned as a json array.
    */

    // the pointer that actually point to a struct which we want to unmarshal to 
    var pointer interface{} 

    switch o.conf.GetString("Operation") {
    // if "create", the data is a map
    case C:     
        pointer = o.conf.Get("C_obj_pointer").Interface()

        // delete all foreign key data so that they wouldn't be unmarshal
        for _, field_name := range getForeignKeys(pointer, "all") {
            data.Del(field_name)
        }

    // if "read", the data is a slice
    case R:     
        pointer = o.conf.Get("R_obj_pointer").Interface()

        container_type := reflect.TypeOf(pointer)
        if container_type.Kind() == reflect.Ptr {
            container_type = container_type.Elem()
        }

        if container_type.Kind() == reflect.Struct {
            if data.Length() > 1 {     // want one object, but got many
                err = errors.New("Too many objects found!")
            } else if data.Length() == 1 {
                data = data.GetIndex(0)

                for _, field_name := range getForeignKeys(pointer, "toOne") {
                    field_data := data.Get(field_name).GetIndex(0)
                    data.Set(field_name, field_data.Interface())
                }
            }
        } else if container_type.Kind() == reflect.Slice {
            for i := 0; i < data.Length(); i++ {
                obj := data.GetIndex(i)

                // convert toOne field from array to the object
                for _, field_name := range getForeignKeys(pointer, "toOne") {
                    field_data := obj.Get(field_name).GetIndex(0)
                    obj.Set(field_name, field_data.Interface())
                }
            }
        } else {
            err = errors.New("Invalid variable type to hold result!")
        }
    } // switch

    if err != nil {
        return
    }

    //data.P()
    //err = data.ToStruct(pointer)
    result_data = data

    return
}

func (o *Orm) beforeDo() {

	switch o.conf.GetString("Operation") {
    case C:
    case U:
    case R:
        typ := reflect.TypeOf(o.conf.Get("R_obj_pointer").Interface())
        for {
            if typ.Kind() == reflect.Struct {
                break
            }
            typ = typ.Elem()   
        }

        // replace the field name in "condition" with field tag
        if condition, found := o.CheckGet("condition"); found {
            for _, key := range condition.Keys() {
                if f, ok := typ.FieldByName(key); ok {
                    o.Get("condition").RenameKey(key, f.Tag.Get("json"))
                }
            }
        }

        // replace the field name in "fields" with field tag
        if fields, found := o.CheckGet("fields"); found {
            newFields := make([]string, fields.Length())

            for i := 0; i < fields.Length(); i++ {
                key := fields.GetIndex(i).MustString()

                if f, ok := typ.FieldByName(key); ok {
                    key = f.Tag.Get("json")
                }
                newFields[i] = key
            }
            o.EnSet(newFields, "fields")
        }
    }

}

//func findRealObj(data *json.Json, name string) (result *json.Json, err error) {
//    /*
//    If search by "bk_inst_name" and use the orm.EQ operator, 
//    the operator would be changed to orm.REGEX in the Orm.
//    Filter method and the response would contain more than one object,
//    we should find the real one before we return the result
//    */
//
//    info := data.GetPath("data", "info")
//
//    for i := 0; i < len(info.MustArray()); i++ {
//        obj := info.GetIndex(i)
//
//        if obj.GetString("bk_inst_name") == name {
//            //data.SetPath([]string{"data", "info"}, []interface{}{obj})
//            //data.SetPath([]string{"data", "count"}, 1)
//            data.SetPath1([]interface{}{obj}, "data", "info")
//            data.SetPath1(1, "data", "count")
//            result = json.FromString(data.ToString())
//            break
//        }
//    }
//
//    if result == nil {
//        err = errors.New("No object found!")
//    }
//
//    return
//}

func (o *Orm) doPut() (result *json.Json, err error) {
var url string

    if o.conf.GetString("Operation") == U {
        url = GenURL(apiconf.URL, "inst", apiconf.SUPPLIER_ID,
                     o.conf.GetString("ObjId"), o.conf.GetInt("U_InstId"))
    } else {
        err = errors.New("update must use PUT!")
    }
    
    _, body, errs := client.Put(url).Send(o.ToString()).End()

    result = body

    if len(errs) != 0 {
        err = errs[0]
    }

    return
}

func (o *Orm) doPost() (result *json.Json, err error) {
    var url string

    if o.conf.GetString("Operation") != R {
        url = GenURL(apiconf.URL, "inst", apiconf.SUPPLIER_ID,
                     o.conf.GetString("ObjId"))

        if o.conf.GetString("Operation") == U {
            url = GenURL(url, o.conf.GetInt("U_InstId"))
        }
    } else {
        url = GenURL(apiconf.URL, "inst/search/owner",
                     apiconf.SUPPLIER_ID, "object", 
                     o.conf.GetString("ObjId"))
    }
    
    _, body, errs := client.Post(url).Send(o.ToString()).End()

    result = body

    if len(errs) != 0 {
        err = errs[0]
    }

    return
}

func (o *Orm) doDelete() (result *json.Json, err error) {
    url := GenURL(apiconf.URL, "inst", apiconf.SUPPLIER_ID,
                  o.conf.GetString("ObjId"), 
                  o.conf.GetInt("D_InstId"))

    _, result, errs := client.Delete(url).End()

    if len(errs) != 0 {
        err = errs[0]
    }

    return
}
