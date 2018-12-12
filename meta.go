package bkorm

import (
    //"fmt"
    "reflect"

    json "github.com/jabbawockeez/go-simplejson"
    //. "github.com/jabbawockeez/go-utils"
)

type BaseObj struct {
    Referer *json.Json
}

func Bind(data map[string]interface{}) {
    // first of all, we need to generate our "struct_objid" map, 
    // and it is used below to generate the "foreignFields" of each binded struct
    for objid, obj := range data {
        typ := reflect.TypeOf(obj)

        if typ.Kind() == reflect.Ptr {
            typ = typ.Elem()
        }

        Meta.EnSet(objid, "struct_objid", typ.String())
        Meta.EnSet(typ.String(), "struct_objid", objid)
    }

    // generate the "foreignFields" of each binded struct
    for objid, obj := range data {
        toOne, toMany := genForeignFieldsInfo(obj)
        Meta.EnSet(toOne, "foreignFields", objid, "toOne")
        Meta.EnSet(toMany, "foreignFields", objid, "toMany")
    }
}

func bkObjIdOf(v interface{}) (bk_obj_id string) {
    t := reflect.TypeOf(v)

    for {
        if t.Kind() == reflect.Struct {
            break
        } 
        t = t.Elem()
    }
    bk_obj_id = Meta.GetString("struct_objid", t.String())

    return
}

// get referer of current struct(bk_obj_id)
func getRefObjIds(ObjId string) (result []string) {
    result = []string{}

    for _, bindedObjId := range Meta.Get("foreignFields").Keys() {
        for _, k := range getForeignKeys(bindedObjId, "all") {
            // if there's one "k"(which is a foreign field in the bindedObjId).
            // then add the bindedObjId to the result
            if k == ObjId {
                result = append(result, bindedObjId)
                break
            }
        }
    }

    return
}

func getForeignKeys(obj interface{}, typ string) (result []string) {
    // "typ" can be "toOne", "toMany" or "all"

    var bk_obj_id string

    t := reflect.TypeOf(obj)

    if t.Kind() == reflect.String {
        bk_obj_id = obj.(string)
    } else {
        bk_obj_id = bkObjIdOf(obj)
    }

    if typ != "all" {
        return Meta.GetPath("foreignFields", bk_obj_id, typ).MustStringArray()
    }

    one := Meta.GetStringArray("foreignFields", bk_obj_id, "toOne")
    many := Meta.GetStringArray("foreignFields", bk_obj_id, "toMany")

    one = append(one, many...)

    return one
}

func genForeignFieldsInfo(obj interface{}) (toOne, toMany []string){
    toOne = []string{}
    toMany = []string{}

    v := reflect.ValueOf(obj)
    if v.Kind() == reflect.Ptr {
        v = v.Elem()
    }

    for i := 0; i < v.NumField(); i++ {
        field_typ := v.Field(i).Type()
        
        if field_typ.Kind() == reflect.Ptr {
            field_typ = field_typ.Elem()
        } 

        switch field_typ.Kind() {
        case reflect.Struct:
            if field_typ == reflect.TypeOf(new(BaseObj)).Elem() {
                continue
            }
            toOne = append(toOne, field_typ.String())
        case reflect.Slice:
            for {
                if field_typ.Kind() == reflect.Struct {
                    break
                } 
                field_typ = field_typ.Elem()
            }
            toMany = append(toMany, field_typ.String())
        }
    }

    for i, struct_name := range toOne {
        bk_obj_id := Meta.GetString("struct_objid", struct_name)
        if bk_obj_id == "" {
            panic(struct_name + " not bind yet!")
        } else {
            toOne[i] = bk_obj_id
        }
    }
    for i, struct_name := range toMany {
        bk_obj_id := Meta.GetString("struct_objid", struct_name)
        if bk_obj_id == "" {
            panic(struct_name + " not bind yet!")
        } else {
            toMany[i] = bk_obj_id
        }
    }

    return
}
